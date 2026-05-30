import json
import logging
import os
import shutil
import zipfile
from datetime import datetime, timezone
from src.pipeline.constants import (
    ATP_DIR,
    GEOJSON_DIR,
    SPLIT_DIR,
    PARQUET_PATH,
    SPIDERS_PATH,
    ATP_HISTORY_URL,
)
import duckdb
import requests

from src.pipeline._db import connect, last_import_date, record_import
from src.pipeline.ndgeojson_to_parquet import convert_to_parquet
from src.utils import delete_file_if_exists, download_large_file


logger = logging.getLogger(__name__)


def download_atp():
    conn = connect()
    try:
        last_date = last_import_date(conn, "atp")

        resp = requests.get(ATP_HISTORY_URL, timeout=30)
        resp.raise_for_status()
        runs = list(reversed(resp.json()))

        ATP_DIR.mkdir(parents=True, exist_ok=True)

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

            delete_file_if_exists(ATP_DIR / "output.zip")
            delete_file_if_exists(SPIDERS_PATH)

            download_large_file(zip_url, ATP_DIR / "output.zip")

            if stats_url:
                stats_path = ATP_DIR / "stats.json"
                download_large_file(stats_url, stats_path)
                with open(stats_path) as infile, open(SPIDERS_PATH, "w") as out:
                    out.write(json.dumps(json.loads(infile.read())["results"]))
                stats_path.unlink()

            logger.info("Downloaded ATP run %s", run_id)
            return

        raise RuntimeError("No ATP run could be downloaded")

    finally:
        conn.close()


def extract_atp():
    zip_path = ATP_DIR / "output.zip"
    if not zip_path.exists():
        logger.info("No ATP zip found, skipping extraction")
        return

    if GEOJSON_DIR.exists():
        shutil.rmtree(GEOJSON_DIR)
    GEOJSON_DIR.mkdir(parents=True)

    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(GEOJSON_DIR)

    geojson_files = list(GEOJSON_DIR.rglob("*.geojson"))
    if not geojson_files:
        raise FileNotFoundError(f"No .geojson files found in {GEOJSON_DIR}")

    for f in geojson_files:
        if f.parent != GEOJSON_DIR:
            f.rename(GEOJSON_DIR / f.name)

    logger.info("Extracted ATP zip (%d geojson files)", len(geojson_files))


def create_parquet_atp():
    """Step 5: Create parquet from split NDJSON files."""
    if not SPLIT_DIR.exists():
        raise FileNotFoundError(f"No split directory at {SPLIT_DIR}")
    delete_file_if_exists(PARQUET_PATH)
    convert_to_parquet(SPLIT_DIR, PARQUET_PATH)
    logger.info("Created parquet from NDJSON files")


def import_atp():
    conn = connect()
    try:
        if not PARQUET_PATH.exists():
            raise FileNotFoundError(
                f"No parquet file at {PARQUET_PATH} — atp-parquet must run first"
            )

        parquet_mtime = datetime.fromtimestamp(
            PARQUET_PATH.stat().st_mtime, tz=timezone.utc
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
                    CASE
                        WHEN SUBSTRING(properties->>'$.addr:postcode', 1, 2) IN ('97', '98')
                            THEN SUBSTRING(properties->>'$.addr:postcode', 1, 3)
                        ELSE SUBSTRING(properties->>'$.addr:postcode', 1, 2)
                    END AS departement_number,
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
                FROM read_parquet('{PARQUET_PATH}')
                WHERE properties->>'$.addr:country' = 'FR'
                    AND geom IS NOT NULL
                    AND REGEXP_MATCHES(COALESCE(properties->>'$.addr:postcode', ''), '^(2[AB]|[0-9]{{2}})[0-9]{{3}}$')
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
                        ON atp_fr (normalize_phone(phone));
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
                FROM read_json('{SPIDERS_PATH}')
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
        path = ATP_DIR / name
        if not path.exists():
            continue
        if path.is_dir():
            shutil.rmtree(path)
        else:
            path.unlink()
        logger.info("Cleaned up %s", path)
