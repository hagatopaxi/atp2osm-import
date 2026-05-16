import json
import logging
import os
import shutil
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import duckdb

logger = logging.getLogger(__name__)

_WORKERS = int(os.getenv("PIPELINE_WORKERS") or max(1, (os.cpu_count() or 4) // 2))

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

        with ThreadPoolExecutor(max_workers=_WORKERS) as executor:
            parts = list(executor.map(convert_one, enumerate(files)))

        parts = [p for p in parts if p.exists() and p.stat().st_size > 0]
        if not parts:
            raise RuntimeError("No parquet parts generated")

        # Step 2: merge all mini parquets → final parquet (parquet→parquet streams fine)
        logger.info("Step 2/2 — merging %d parquet parts...", len(parts))
        glob_parts = (duck_temp / "*.parquet").as_posix()

        with duckdb.connect() as con:
            con.load_extension("spatial")
            con.execute(f"SET threads={_WORKERS}")
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
        new_md = {**existing_md, b"geo": json.dumps(geo_meta, ensure_ascii=False).encode()}
        pq.write_table(table.replace_schema_metadata(new_md), str(output_path), compression="ZSTD")

    except ImportError:
        logger.info("pyarrow not available — skipping GeoParquet metadata")
