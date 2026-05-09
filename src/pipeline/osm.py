import logging
import os
import subprocess
from datetime import datetime
from pathlib import Path

import requests

from src.pipeline._db import connect, last_import_date, record_import
from src.utils import delete_file_if_exists, download_large_file

logger = logging.getLogger(__name__)

_PROJECT_ROOT = Path(__file__).parent.parent.parent
_PBF_PATH = _PROJECT_ROOT / "data" / "osm" / "france-latest.osm.pbf"
_GEOFABRIK_URL = "https://download.geofabrik.de/europe/france-latest.osm.pbf"
_GEOFABRIK_STATE_URL = "https://download.geofabrik.de/europe/france-updates/state.txt"
_OSM2PGSQL_IMAGE = "docker.io/iboates/osm2pgsql:latest"


def _geofabrik_timestamp():
    resp = requests.get(_GEOFABRIK_STATE_URL, timeout=30)
    resp.raise_for_status()
    for line in resp.text.splitlines():
        if line.startswith("timestamp="):
            ts = line[len("timestamp=") :].replace("\\:", ":")
            return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    raise ValueError("Could not parse Geofabrik state.txt")


def download_pbf():
    geofabrik_ts = _geofabrik_timestamp()

    conn = connect()
    try:
        last_date = last_import_date(conn, "osm")
    finally:
        conn.close()

    if last_date and last_date >= geofabrik_ts:
        logger.info(
            "OSM data already up-to-date (%s), skipping download", last_date.date()
        )
        return

    if _PBF_PATH.exists():
        logger.info("PBF already present, skipping download")
        return

    logger.info("New OSM data available (%s), downloading...", geofabrik_ts.date())
    _PBF_PATH.parent.mkdir(parents=True, exist_ok=True)
    try:
        download_large_file(_GEOFABRIK_URL, _PBF_PATH)
    except Exception:
        delete_file_if_exists(_PBF_PATH)
        raise
    logger.info("Download complete")


def run_osm2pgsql():
    if not _PBF_PATH.exists():
        logger.info("No PBF file found, skipping osm2pgsql")
        return

    logger.info("Importing OSM PBF into PostGIS...")
    subprocess.run(
        [
            "podman",
            "run",
            "--rm",
            "--network",
            "host",
            "-v",
            f"{_PROJECT_ROOT}/osm2pgsql:/osm2pgsql:ro,Z",
            "-v",
            f"{_PROJECT_ROOT}/data:/data:Z",
            "-e",
            f"PGPASSWORD={os.getenv('OSM_DB_PASSWORD')}",
            _OSM2PGSQL_IMAGE,
            "osm2pgsql",
            "--output",
            "flex",
            "-S",
            "/osm2pgsql/generic.lua",
            "-d",
            os.getenv("OSM_DB_NAME"),
            "-U",
            os.getenv("OSM_DB_USER"),
            "-H",
            os.getenv("OSM_DB_HOST"),
            "-P",
            os.getenv("OSM_DB_PORT"),
            "/data/osm/france-latest.osm.pbf",
        ],
        check=True,
    )
    _PBF_PATH.unlink()
    logger.info("osm2pgsql import complete")


def setup_mv_places():
    geofabrik_ts = _geofabrik_timestamp()
    conn = connect()
    try:
        last_date = last_import_date(conn, "osm")
        if last_date and last_date >= geofabrik_ts:
            logger.info("OSM views already up-to-date (%s), skipping", last_date.date())
            record_import(conn, "osm", last_date, "skipped")
            return

        try:
            with conn.cursor() as cur:
                cur.execute("SELECT srid FROM spatial_ref_sys WHERE srid=9794;")
                if not cur.fetchone():
                    logger.info("Inserting EPSG/9794 projection")
                    cur.execute("""
                        INSERT INTO spatial_ref_sys (srid, auth_name, auth_srid, srtext, proj4text)
                        VALUES(9794, 'EPSG', 9794,
                            'PROJCS["RGF93_v2b_Lambert-93",GEOGCS["RGF93_v2b",DATUM["Reseau_Geodesique_Francais_1993_v2b",SPHEROID["GRS_1980",6378137.0,298.257222101]],PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]],PROJECTION["Lambert_Conformal_Conic"],PARAMETER["False_Easting",700000.0],PARAMETER["False_Northing",6600000.0],PARAMETER["Central_Meridian",3.0],PARAMETER["Standard_Parallel_1",49.0],PARAMETER["Standard_Parallel_2",44.0],PARAMETER["Latitude_Of_Origin",46.5],UNIT["Meter",1.0]]',
                            '+proj=lcc +lat_0=46.5 +lon_0=3 +lat_1=49 +lat_2=44 +x_0=700000 +y_0=6600000 +ellps=GRS80 +units=m +no_defs +type=crs'
                        )
                    """)

                cur.execute("DROP MATERIALIZED VIEW IF EXISTS mv_places CASCADE;")
                logger.info("Creating mv_places and indexes...")
                cur.execute("""
                    CREATE MATERIALIZED VIEW mv_places AS
                    SELECT
                        node_id                                              AS osm_id,
                        'node'                                               AS node_type,
                        tags                                                 AS tags,
                        tags->>'name'                                        AS name,
                        tags->>'brand:wikidata'                              AS brand_wikidata,
                        tags->>'brand'                                       AS brand,
                        tags->>'addr:city'                                   AS city,
                        tags->>'addr:postcode'                               AS postcode,
                        tags->>'opening_hours'                               AS opening_hours,
                        COALESCE(tags->>'website', tags->>'contact:website') AS website,
                        COALESCE(tags->>'phone', tags->>'contact:phone')     AS phone,
                        COALESCE(tags->>'email', tags->>'contact:email')     AS email,
                        version,
                        NULL::jsonb                                          AS members,
                        ST_Transform(geom, 9794)                             AS geom_9794,
                        geom
                    FROM points

                    UNION ALL

                    SELECT
                        area_id                                              AS osm_id,
                        CASE osm_type WHEN 'W' THEN 'way' ELSE 'relation' END AS node_type,
                        tags                                                 AS tags,
                        tags->>'name'                                        AS name,
                        tags->>'brand:wikidata'                              AS brand_wikidata,
                        tags->>'brand'                                       AS brand,
                        tags->>'addr:city'                                   AS city,
                        tags->>'addr:postcode'                               AS postcode,
                        tags->>'opening_hours'                               AS opening_hours,
                        COALESCE(tags->>'website', tags->>'contact:website') AS website,
                        COALESCE(tags->>'phone', tags->>'contact:phone')     AS phone,
                        COALESCE(tags->>'email', tags->>'contact:email')     AS email,
                        version,
                        members,
                        ST_Transform(geom, 9794)                             AS geom_9794,
                        geom
                    FROM polygons
                """)

                cur.execute("""
                    CREATE INDEX IF NOT EXISTS mv_places_geom_9794_idx
                        ON mv_places USING GIST (geom_9794);
                    CREATE INDEX IF NOT EXISTS mv_places_brand_wikidata_idx
                        ON mv_places ((brand_wikidata));
                    CREATE INDEX IF NOT EXISTS mv_places_brand_lower_idx
                        ON mv_places (LOWER(brand));
                    CREATE INDEX IF NOT EXISTS mv_places_name_lower_idx
                        ON mv_places (LOWER(name));
                    CREATE INDEX IF NOT EXISTS mv_places_website_norm_idx
                        ON mv_places (LOWER(REGEXP_REPLACE(website, '^https?://', '', 'i')));
                    CREATE INDEX IF NOT EXISTS mv_places_phone_norm_idx
                        ON mv_places (REGEXP_REPLACE(REGEXP_REPLACE(phone, '^\+33', '0'), '\s+', '', 'g'));
                    CREATE INDEX IF NOT EXISTS mv_places_email_lower_idx
                        ON mv_places (LOWER(email));
                """)

            conn.commit()
            record_import(conn, "osm", geofabrik_ts, "success")
            logger.info("mv_places created (data date: %s)", geofabrik_ts.date())

        except Exception:
            logger.exception("setup_mv_places failed")
            try:
                record_import(conn, "osm", None, "error")
            except Exception:
                pass
            raise
    finally:
        conn.close()
