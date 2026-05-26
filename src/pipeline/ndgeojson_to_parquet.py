"""
ATP GeoJSON to Parquet Conversion Pipeline

This module handles the conversion of ATP (All The Places) GeoJSON data to Parquet format.

Background:
The ATP server's parquet generation is buggy because some GeoJSON files are too large
(~2 GB) for DuckDB to load directly. This module implements a multi-step workflow that
can handle GeoJSON files of any size.

Workflow:
1. convert_geojson_to_ndgeojson: Converts FeatureCollection GeoJSON to NDJSON format
   (one feature per line). This step is necessary because the original GeoJSON files
   are in FeatureCollection format which is harder to process in chunks.

2. split_ndgeojson: Splits large NDJSON files (>16 MB) into smaller chunks. This ensures
   that each file can be safely loaded into memory and processed by DuckDB without
   hitting memory limits.

3. convert_to_parquet: Converts the split NDJSON files to Parquet format using DuckDB.
   This is done in two phases:
   - Phase 1: Each NDJSON chunk is converted to a mini Parquet file in parallel
   - Phase 2: All mini Parquet files are merged into a single final Parquet file

4. _write_geoparquet_metadata: Adds GeoParquet metadata to the final output for
   better interoperability with geospatial tools.

The 16 MB chunk size is chosen because:
- It's small enough to avoid memory issues when processing
- It's large enough to maintain good performance (fewer files to merge)
- It works well with DuckDB's default memory limits

This approach, while more complex than direct conversion, ensures reliability
regardless of the input GeoJSON file sizes.
"""

import json
import logging
import shutil
import duckdb
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from src.pipeline.constants import (
    GEOJSON_DIR,
    MAX_FILE_SIZE,
    NDGEOJSON_DIR,
    SPLIT_DIR,
    WORKERS,
)

logger = logging.getLogger(__name__)


_NDJSON_COLS = "{id: 'VARCHAR', properties: 'JSON', geometry: 'JSON'}"


def convert_to_parquet(input_dir: Path, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if output_path.exists():
        output_path.unlink()

    files = [f for f in sorted(input_dir.glob("*.geojson")) if f.stat().st_size > 0]
    if not files:
        raise FileNotFoundError(f"No non-empty .geojson files found in {input_dir}")

    duck_temp = output_path.parent / ".duckdb_temp"
    duck_temp.mkdir(exist_ok=True)

    try:
        # Pre-install extension once to avoid races in threads
        with duckdb.connect() as con:
            con.install_extension("spatial")

        # Step 1: each NDJSON file → mini parquet (parallelized, max 16 MB input each)
        logger.info("Step 1/2 — converting %d NDJSON files to parquet...", len(files))

        def convert_one(args: tuple) -> Path:
            i, file_path = args
            out = duck_temp / f"part_{i:06d}.parquet"
            logger.info("[%d/%d] %s", i + 1, len(files), file_path.name)
            _ndjson_to_parquet(file_path, out)
            return out

        with ThreadPoolExecutor(max_workers=WORKERS) as executor:
            parts = list(executor.map(convert_one, enumerate(files)))

        parts = [p for p in parts if p.exists() and p.stat().st_size > 0]
        if not parts:
            raise RuntimeError("No parquet parts generated")

        # Step 2: merge all mini parquets → final parquet (parquet→parquet streams fine)
        logger.info("Step 2/2 — merging %d parquet parts...", len(parts))
        glob_parts = (duck_temp / "*.parquet").as_posix()

        with duckdb.connect() as con:
            con.load_extension("spatial")
            con.execute(f"SET threads={WORKERS}")
            con.execute("SET memory_limit='2GB'")
            con.execute(f"""
                COPY (SELECT * FROM read_parquet('{glob_parts}'))
                TO '{str(output_path)}'
                (FORMAT PARQUET, COMPRESSION 'ZSTD')
            """)

            bbox_res = con.execute(f"""
                SELECT
                    min(ST_XMin(geom)), min(ST_YMin(geom)),
                    max(ST_XMax(geom)), max(ST_YMax(geom))
                FROM read_parquet('{str(output_path)}')
            """).fetchone()

            geom_types = [
                t[0]
                for t in con.execute(
                    f"SELECT DISTINCT ST_GeometryType(geom) FROM read_parquet('{str(output_path)}')"
                ).fetchall()
                if t and t[0] is not None
            ]

    finally:
        shutil.rmtree(duck_temp, ignore_errors=True)

    _write_geoparquet_metadata(output_path, bbox_res, geom_types)
    logger.info("Created %s", output_path)


def _ndjson_to_parquet(file_path: Path, out_path: Path) -> None:
    with duckdb.connect() as con:
        con.load_extension("spatial")
        con.execute("SET memory_limit='512MB'")
        con.execute("SET threads=1")
        con.execute(f"""
            COPY (
                SELECT id, properties, geom,
                    {{
                        'xmin': ST_XMin(geom),
                        'ymin': ST_YMin(geom),
                        'xmax': ST_XMax(geom),
                        'ymax': ST_YMax(geom)
                    }} AS bbox
                FROM (
                    SELECT
                        id,
                        properties,
                        ST_GeomFromGeoJSON(geometry::VARCHAR) AS geom
                    FROM read_json('{file_path.as_posix()}',
                        format='newline_delimited',
                        columns={_NDJSON_COLS},
                        maximum_object_size=16777216)
                    WHERE geometry IS NOT NULL
                )
            ) TO '{out_path.as_posix()}' (FORMAT PARQUET, COMPRESSION 'ZSTD')
        """)


def _write_geoparquet_metadata(output_path: Path, bbox_res, geom_types: list) -> None:
    try:
        import pyarrow.parquet as pq

        if not bbox_res or any(v is None for v in bbox_res):
            return

        table = pq.read_table(str(output_path), memory_map=True)
        xmin, ymin, xmax, ymax = bbox_res
        geo_meta = {
            "version": "1.1.0",
            "primary_column": "geom",
            "columns": {
                "geom": {
                    "encoding": "WKB",
                    "geometry_types": geom_types or ["Unknown"],
                    "crs": "EPSG:4326",
                }
            },
            "bbox": [xmin, ymin, xmax, ymax],
        }
        existing_md = table.schema.metadata or {}
        new_md = {
            **existing_md,
            b"geo": json.dumps(geo_meta, ensure_ascii=False).encode(),
        }
        pq.write_table(
            table.replace_schema_metadata(new_md), str(output_path), compression="ZSTD"
        )

    except ImportError:
        logger.info("pyarrow not available — skipping GeoParquet metadata")


def convert_geojson_to_ndgeojson(geojson_dir: Path, ndgeojson_dir: Path) -> None:
    """Convert FeatureCollection GeoJSON to NDJSON (one feature per line)."""
    if ndgeojson_dir.exists():
        shutil.rmtree(ndgeojson_dir)
    ndgeojson_dir.mkdir(parents=True)

    files = sorted(geojson_dir.glob("*.geojson"))
    if not files:
        raise FileNotFoundError(f"No .geojson files in {geojson_dir}")

    with ThreadPoolExecutor(max_workers=WORKERS) as executor:
        futures = [
            executor.submit(_geojson_to_ndgeojson_single, f, ndgeojson_dir)
            for f in files
        ]
        for fut in futures:
            fut.result()

    logger.info("Converted FC geojson to NDJSON")


def _geojson_to_ndgeojson_single(file_path: Path, NDGEOJSON_DIR: Path) -> None:
    if file_path.stat().st_size == 0:
        return

    out_path = NDGEOJSON_DIR / file_path.name
    written = 0

    with open(file_path, "rb") as f_in, open(out_path, "wb") as f_out:
        first = True
        prev = None
        for line in f_in:
            if first:
                first = False
                continue  # skip FeatureCollection header
            if prev is not None:
                clean = prev.rstrip()
                if clean.endswith(b","):
                    clean = clean[:-1]
                if clean:
                    f_out.write(clean + b"\n")
                    written += 1
            prev = line
        # prev is the last line `]}` — skip it

    if written == 0:
        out_path.unlink()
        logger.info("Skipping %s: no features", file_path.name)


def split_ndgeojson(NDGEOJSON_DIR: Path, split_dir: Path) -> None:
    """Split NDJSON files larger than 16 MB; move smaller files as-is."""
    if split_dir.exists():
        shutil.rmtree(split_dir)
    split_dir.mkdir(parents=True)

    files = sorted(NDGEOJSON_DIR.glob("*.geojson"))
    if not files:
        raise FileNotFoundError(f"No .geojson files in {NDGEOJSON_DIR}")

    with ThreadPoolExecutor(max_workers=WORKERS) as executor:
        futures = [
            executor.submit(_split_or_move_ndgeojson, f, split_dir) for f in files
        ]
        for fut in futures:
            fut.result()

    logger.info("Split complete")


def _split_or_move_ndgeojson(file_path: Path, split_dir: Path) -> None:
    if file_path.stat().st_size <= MAX_FILE_SIZE:
        shutil.move(str(file_path), split_dir / file_path.name)
        return
    _split_ndgeojson_file(file_path, split_dir)
    file_path.unlink()


def _split_ndgeojson_file(file_path: Path, split_dir: Path) -> None:
    base_name = file_path.stem
    data = file_path.read_bytes()
    total = len(data)

    start = 0
    chunk_num = 1

    while start < total:
        end = start + MAX_FILE_SIZE
        if end >= total:
            chunk_path = split_dir / f"{base_name}_{chunk_num}.geojson"
            chunk_path.write_bytes(data[start:])
            break

        # Find the last \n strictly before the 16 MB boundary
        split_at = data.rfind(b"\n", start, end)
        if split_at == -1 or split_at <= start:
            # Line longer than 16 MB — hard split at boundary
            split_at = end - 1

        chunk_path = split_dir / f"{base_name}_{chunk_num}.geojson"
        chunk_path.write_bytes(data[start : split_at + 1])
        chunk_num += 1
        start = split_at + 1

    logger.info("Split %s into %d chunks", file_path.name, chunk_num)


# Wrapper functions for pipeline runner (no parameters)
def convert_atp() -> None:
    """Step: Convert FeatureCollection GeoJSON to NDJSON."""
    convert_geojson_to_ndgeojson(GEOJSON_DIR, NDGEOJSON_DIR)


def split_atp() -> None:
    """Step: Split NDJSON files larger than 16 MB."""
    split_ndgeojson(NDGEOJSON_DIR, SPLIT_DIR)
