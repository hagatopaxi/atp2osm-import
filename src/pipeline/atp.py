import json
import logging
import os
import shutil
import zipfile
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import requests

from src.pipeline._db import connect, last_import_date, record_import
from src.pipeline.ndgeojson_to_parquet import convert_to_parquet
from src.utils import delete_file_if_exists, download_large_file

_MAX_FILE_SIZE = 16 * 1024 * 1024  # 16 MB
_WORKERS = int(os.getenv("PIPELINE_WORKERS") or max(1, (os.cpu_count() or 4) // 2))

logger = logging.getLogger(__name__)

_PROJECT_ROOT = Path(__file__).parent.parent.parent
_ATP_DIR = _PROJECT_ROOT / "data" / "atp"
_GEOJSON_DIR = _ATP_DIR / "geojson"
_NDGEOJSON_DIR = _ATP_DIR / "ndgeojson"
_SPLIT_DIR = _ATP_DIR / "split"
_PARQUET_PATH = _ATP_DIR / "latest.parquet"
_SPIDERS_PATH = _ATP_DIR / "spiders.json"
_ATP_HISTORY_URL = "https://data.alltheplaces.xyz/runs/history.json"


def download_atp():
    conn = connect()
    try:
        last_date = last_import_date(conn, "atp")

        resp = requests.get(_ATP_HISTORY_URL, timeout=30)
        resp.raise_for_status()
        runs = list(reversed(resp.json()))

        _ATP_DIR.mkdir(parents=True, exist_ok=True)

        for run in runs:
            end_time_raw = run.get("end_time")
            end_time = (
                datetime.fromisoformat(end_time_raw.replace("Z", "+00:00"))
                if end_time_raw
                else None
            )
            parquet_url = run.get("parquet_url")
            run_id = run.get("run_id")

            if not parquet_url:
                continue
            if last_date is not None and end_time is not None and end_time <= last_date:
                logger.info(
                    "ATP already up-to-date (run %s, %s), skipping",
                    run_id,
                    end_time.date(),
                )
                return

            zip_url = run.get("output_url")
            stats_url = run.get("stats_url")

            delete_file_if_exists(_ATP_DIR / "output.zip")
            delete_file_if_exists(_SPIDERS_PATH)

            download_large_file(zip_url, _ATP_DIR / "output.zip")

            if stats_url:
                stats_path = _ATP_DIR / "stats.json"
                download_large_file(stats_url, stats_path)
                with open(stats_path) as infile, open(_SPIDERS_PATH, "w") as out:
                    out.write(json.dumps(json.loads(infile.read())["results"]))
                stats_path.unlink()

            logger.info("Downloaded ATP run %s", run_id)
            return

        raise RuntimeError("No ATP run could be downloaded")

    finally:
        conn.close()


def extract_atp():
    zip_path = _ATP_DIR / "output.zip"
    if not zip_path.exists():
        logger.info("No ATP zip found, skipping extraction")
        return

    if _GEOJSON_DIR.exists():
        shutil.rmtree(_GEOJSON_DIR)
    _GEOJSON_DIR.mkdir(parents=True)

    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(_GEOJSON_DIR)

    geojson_files = list(_GEOJSON_DIR.rglob("*.geojson"))
    if not geojson_files:
        raise FileNotFoundError(f"No .geojson files found in {_GEOJSON_DIR}")

    for f in geojson_files:
        if f.parent != _GEOJSON_DIR:
            f.rename(_GEOJSON_DIR / f.name)

    logger.info("Extracted ATP zip (%d geojson files)", len(geojson_files))


def convert_atp():
    """Step 3: Convert FeatureCollection GeoJSON to NDJSON (one feature per line)."""
    if _NDGEOJSON_DIR.exists():
        shutil.rmtree(_NDGEOJSON_DIR)
    _NDGEOJSON_DIR.mkdir(parents=True)

    files = sorted(_GEOJSON_DIR.glob("*.geojson"))
    if not files:
        raise FileNotFoundError(f"No .geojson files in {_GEOJSON_DIR}")

    with ThreadPoolExecutor(max_workers=_WORKERS) as executor:
        futures = [executor.submit(_convert_single_file, f) for f in files]
        for fut in futures:
            fut.result()

    logger.info("Converted FC geojson to NDJSON")


def _convert_single_file(file_path: Path):
    if file_path.stat().st_size == 0:
        return

    out_path = _NDGEOJSON_DIR / file_path.name
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


def split_atp():
    """Step 4: Split NDJSON files larger than 16 MB; move smaller files as-is."""
    if _SPLIT_DIR.exists():
        shutil.rmtree(_SPLIT_DIR)
    _SPLIT_DIR.mkdir(parents=True)

    files = sorted(_NDGEOJSON_DIR.glob("*.geojson"))
    if not files:
        raise FileNotFoundError(f"No .geojson files in {_NDGEOJSON_DIR}")

    with ThreadPoolExecutor(max_workers=_WORKERS) as executor:
        futures = [executor.submit(_split_or_move, f) for f in files]
        for fut in futures:
            fut.result()

    logger.info("Split complete")


def _split_or_move(file_path: Path):
    if file_path.stat().st_size <= _MAX_FILE_SIZE:
        shutil.move(str(file_path), _SPLIT_DIR / file_path.name)
        return
    _split_ndjson_file(file_path)
    file_path.unlink()


def _split_ndjson_file(file_path: Path):
    base_name = file_path.stem
    data = file_path.read_bytes()
    total = len(data)

    start = 0
    chunk_num = 1

    while start < total:
        end = start + _MAX_FILE_SIZE
        if end >= total:
            chunk_path = _SPLIT_DIR / f"{base_name}_{chunk_num}.geojson"
            chunk_path.write_bytes(data[start:])
            break

        # Find the last \n strictly before the 16 MB boundary
        split_at = data.rfind(b"\n", start, end)
        if split_at == -1 or split_at <= start:
            # Line longer than 16 MB — hard split at boundary
            split_at = end - 1

        chunk_path = _SPLIT_DIR / f"{base_name}_{chunk_num}.geojson"
        chunk_path.write_bytes(data[start : split_at + 1])
        chunk_num += 1
        start = split_at + 1

    logger.info("Split %s into %d chunks", file_path.name, chunk_num)


def create_parquet_atp():
    """Step 5: Create parquet from split NDJSON files."""
    if not _SPLIT_DIR.exists():
        raise FileNotFoundError(f"No split directory at {_SPLIT_DIR}")
    delete_file_if_exists(_PARQUET_PATH)
    convert_to_parquet(_SPLIT_DIR, _PARQUET_PATH)
    logger.info("Created parquet from NDJSON files")


def import_atp():
    conn = connect()
    try:
        if not _PARQUET_PATH.exists():
            raise FileNotFoundError(
                f"No parquet file at {_PARQUET_PATH} — atp-parquet must run first"
            )

        parquet_mtime = datetime.fromtimestamp(
            _PARQUET_PATH.stat().st_mtime, tz=timezone.utc
        )
        last_date = last_import_date(conn, "atp")

        if last_date is not None and parquet_mtime <= last_date:
            logger.info(
                "Parquet not newer than last import (%s), skipping", last_date.date()
            )
            return

        try:
            with conn.cursor() as cur:
                cur.execute("DROP TABLE IF EXISTS atp_fr CASCADE")
                cur.execute("DROP TABLE IF EXISTS atp_spiders CASCADE")
            conn.commit()

            db_url = (
                f"dbname={os.getenv('OSM_DB_NAME')} "
                f"user={os.getenv('OSM_DB_USER')} "
                f"host={os.getenv('OSM_DB_HOST')} "
                f"password={os.getenv('OSM_DB_PASSWORD')} "
                f"port={os.getenv('OSM_DB_PORT')}"
            )
            ddb = duckdb.connect()
            ddb.execute("INSTALL postgres; LOAD postgres;")
            ddb.execute("INSTALL spatial; LOAD spatial;")
            ddb.execute(f"ATTACH '{db_url}' AS pg (TYPE postgres);")

            logger.info("Creating atp_fr table from parquet...")
            ddb.execute(f"""
                CREATE TABLE pg.atp_fr AS
                SELECT
                    id,
                    properties->>'$.addr:country'    AS country,
                    properties->>'$.addr:city'        AS city,
                    properties->>'$.addr:postcode'    AS postcode,
                    SUBSTRING(properties->>'$.addr:postcode', 1, 2) AS departement_number,
                    properties->>'$.brand:wikidata'   AS brand_wikidata,
                    properties->>'$.brand'            AS brand,
                    properties->>'$.name'             AS name,
                    properties->>'$.opening_hours'    AS opening_hours,
                    properties->>'$.website'          AS website,
                    properties->>'$.phone'            AS phone,
                    properties->>'$.email'            AS email,
                    properties->>'$.end_date'         AS end_date,
                    properties->>'$.@spider'          AS spider_id,
                    NULL::VARCHAR                     AS source_type,
                    properties->>'$.@source_uri'      AS source_uri,
                    ST_AsGeoJSON(geom)                AS geom
                FROM read_parquet('{_PARQUET_PATH}')
                WHERE properties->>'$.addr:country' = 'FR'
                    AND geom IS NOT NULL
                    AND REGEXP_MATCHES(COALESCE(SUBSTRING(properties->>'$.addr:postcode', 1, 2), ''), '^[0-9]+$')
            """)

            logger.info("Creating indexes for atp_fr...")
            with conn.cursor() as cur:
                cur.execute("DELETE FROM atp_fr WHERE postcode IS NULL;")
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS atp_fr_geom_idx
                        ON atp_fr USING GIST (ST_Transform(ST_GeomFromGeoJSON(geom), 9794));
                    CREATE INDEX IF NOT EXISTS atp_fr_brand_wikidata_idx
                        ON atp_fr (brand_wikidata);
                    CREATE INDEX IF NOT EXISTS atp_fr_brand_lower_idx
                        ON atp_fr (LOWER(brand));
                    CREATE INDEX IF NOT EXISTS atp_fr_name_lower_idx
                        ON atp_fr (LOWER(name));
                    CREATE INDEX IF NOT EXISTS atp_fr_website_norm_idx
                        ON atp_fr (LOWER(REGEXP_REPLACE(website, '^https?://', '', 'i')));
                    CREATE INDEX IF NOT EXISTS atp_fr_phone_norm_idx
                        ON atp_fr (REGEXP_REPLACE(REGEXP_REPLACE(phone, '^\+33', '0'), '\s+', '', 'g'));
                    CREATE INDEX IF NOT EXISTS atp_fr_email_lower_idx
                        ON atp_fr (LOWER(email));
                    CREATE INDEX IF NOT EXISTS atp_fr_departement_number_idx
                        ON atp_fr (departement_number);
                    CREATE INDEX IF NOT EXISTS atp_fr_spider_idx
                        ON atp_fr (spider_id);
                    CREATE INDEX IF NOT EXISTS atp_fr_source_type_idx
                        ON atp_fr (source_type);
                """)
            conn.commit()

            logger.info("Creating atp_spiders table...")
            ddb.execute(f"""
                CREATE TABLE pg.atp_spiders AS
                SELECT *
                FROM read_json('{_SPIDERS_PATH}')
                WHERE spider IN (SELECT DISTINCT spider_id FROM pg.atp_fr)
            """)

            record_import(conn, "atp", parquet_mtime, "success")
            logger.info("ATP import complete (parquet mtime: %s)", parquet_mtime.date())

        except Exception:
            logger.exception("import_atp failed")
            try:
                record_import(conn, "atp", None, "error")
            except Exception:
                pass
            raise

    finally:
        conn.close()


def cleanup_atp():
    for name in ["output.zip", "geojson", "ndgeojson", "split", "stats.json"]:
        path = _ATP_DIR / name
        if not path.exists():
            continue
        if path.is_dir():
            shutil.rmtree(path)
        else:
            path.unlink()
        logger.info("Cleaned up %s", path)
