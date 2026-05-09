import json
import logging
import os
from datetime import datetime
from pathlib import Path

import duckdb
import requests

from src.pipeline._db import connect, last_import_date, record_import
from src.utils import delete_file_if_exists, download_large_file

logger = logging.getLogger(__name__)

_PROJECT_ROOT = Path(__file__).parent.parent.parent
_ATP_DIR = _PROJECT_ROOT / "data" / "atp"
_PARQUET_PATH = _ATP_DIR / "latest.parquet"
_SPIDERS_PATH = _ATP_DIR / "spiders.json"
_ATP_HISTORY_URL = "https://data.alltheplaces.xyz/runs/history.json"


def import_atp():
    conn = connect()
    try:
        last_date = last_import_date(conn, "atp")

        resp = requests.get(_ATP_HISTORY_URL, timeout=30)
        resp.raise_for_status()
        runs = list(reversed(resp.json()))

        _ATP_DIR.mkdir(parents=True, exist_ok=True)

        used_end_time = None
        for run in runs:
            end_time_raw = run.get("end_time")
            end_time = (
                datetime.fromisoformat(end_time_raw.replace("Z", "+00:00"))
                if end_time_raw
                else None
            )
            parquet_url = run.get("parquet_url")
            stats_url = run.get("stats_url")
            run_id = run.get("run_id")

            if not parquet_url:
                continue
            if last_date is not None and end_time is not None and end_time <= last_date:
                logger.info("ATP already up-to-date (run %s, %s), skipping", run_id, end_time.date())
                record_import(conn, "atp", last_date, "skipped")
                return

            try:
                delete_file_if_exists(_PARQUET_PATH)
                stats_path = _ATP_DIR / "stats.json"
                delete_file_if_exists(stats_path)

                download_large_file(parquet_url, _PARQUET_PATH)
                with open(_PARQUET_PATH, "rb") as f:
                    if f.read(4) != b"PAR1":
                        raise ValueError(f"Invalid parquet file for run {run_id}")

                if stats_url:
                    download_large_file(stats_url, stats_path)
                    with open(stats_path) as infile, open(_SPIDERS_PATH, "w") as out:
                        out.write(json.dumps(json.loads(infile.read())["results"]))
                    delete_file_if_exists(stats_path)

                used_end_time = end_time
                logger.info("Downloaded ATP run %s", run_id)
                break

            except Exception as exc:
                logger.warning("Failed to download ATP run %s: %s, trying previous...", run_id, exc)
                delete_file_if_exists(_PARQUET_PATH)
                delete_file_if_exists(stats_path)

        if used_end_time is None:
            raise RuntimeError("No ATP run could be downloaded")

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
                    TRY_CAST(SUBSTRING(properties->>'$.addr:postcode', 1, 2) AS INTEGER) AS departement_number,
                    properties->>'$.brand:wikidata'   AS brand_wikidata,
                    properties->>'$.brand'            AS brand,
                    properties->>'$.name'             AS name,
                    properties->>'$.opening_hours'    AS opening_hours,
                    properties->>'$.website'          AS website,
                    properties->>'$.phone'            AS phone,
                    properties->>'$.email'            AS email,
                    properties->>'$.end_date'         AS end_date,
                    dataset_attributes->>'$.@spider'  AS spider_id,
                    dataset_attributes->>'$.source'   AS source_type,
                    properties->>'$.@source_uri'      AS source_uri,
                    ST_AsGeoJSON(geom)                AS geom
                FROM read_parquet('{_PARQUET_PATH}')
                WHERE properties->>'$.addr:country' = 'FR'
                    AND map_extract(properties, 'addr:postcode') IS NOT NULL
                    AND geom IS NOT NULL
                    AND REGEXP_MATCHES(SUBSTRING(properties->>'$.addr:postcode', 1, 2), '^[0-9]+$')
                    AND TRY_CAST(SUBSTRING(properties->>'$.addr:postcode', 1, 2) AS INTEGER) BETWEEN 1 AND 95
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

            record_import(conn, "atp", used_end_time, "success")
            logger.info("ATP import complete (run date: %s)", used_end_time.date())

        except Exception:
            logger.exception("import_atp failed")
            try:
                record_import(conn, "atp", None, "error")
            except Exception:
                pass
            raise

    finally:
        conn.close()
