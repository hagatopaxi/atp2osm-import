import logging
import os
import subprocess
from datetime import datetime
from email.utils import parsedate_to_datetime
from src.pipeline.constants import (
    PROJECT_ROOT,
    GEOFABRIK_REGIONS,
)

import requests

from src.pipeline._db import connect, last_import_date, record_import
from src.utils import delete_file_if_exists, download_large_file

logger = logging.getLogger(__name__)


def _geofabrik_timestamp(region: dict) -> datetime:
    """Fetch the data timestamp for a region.

    Tries the Geofabrik state.txt first; falls back to the HTTP Last-Modified
    header of the PBF file for regions that don't publish a state file.
    """
    try:
        resp = requests.get(region["state_url"], timeout=30)
        resp.raise_for_status()
        for line in resp.text.splitlines():
            if line.startswith("timestamp="):
                ts = line[len("timestamp="):].replace("\\:", ":")
                return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except Exception:
        pass

    # Fallback: Last-Modified header on the PBF file
    resp = requests.head(region["url"], timeout=30, allow_redirects=True)
    resp.raise_for_status()
    last_modified = resp.headers.get("Last-Modified")
    if last_modified:
        return parsedate_to_datetime(last_modified)

    raise ValueError(f"Cannot determine data timestamp for {region['url']}")


def _newest_geofabrik_timestamp() -> datetime:
    """Return the most recent timestamp across all configured regions.

    We refresh when any region has data newer than our last import,
    so we compare last_import_date against the maximum (newest) timestamp.
    """
    timestamps = []
    for name, region in GEOFABRIK_REGIONS.items():
        try:
            timestamps.append(_geofabrik_timestamp(region))
        except Exception as exc:
            logger.warning("Could not fetch timestamp for %s: %s", name, exc)
    if not timestamps:
        raise RuntimeError("No Geofabrik timestamps could be fetched")
    return max(timestamps)


def download_pbf():
    newest_ts = _newest_geofabrik_timestamp()

    conn = connect()
    try:
        last_date = last_import_date(conn, "osm")

        if last_date and last_date >= newest_ts:
            logger.info(
                "OSM data already up-to-date (last import: %s), skipping download",
                last_date.date(),
            )
            record_import(conn, "osm", last_date, "skipped")
            return
    finally:
        conn.close()

    logger.info("New OSM data available (newest: %s), downloading all regions...", newest_ts.date())
    for name, region in GEOFABRIK_REGIONS.items():
        pbf_path = region["pbf_path"]
        if pbf_path.exists():
            logger.info("PBF %s already present, skipping", name)
            continue
        logger.info("Downloading %s...", name)
        pbf_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            download_large_file(region["url"], pbf_path)
        except Exception:
            delete_file_if_exists(pbf_path)
            raise
        logger.info("Downloaded %s", name)


def run_osm2pgsql():
    pbf_paths = [
        r["pbf_path"]
        for r in GEOFABRIK_REGIONS.values()
        if r["pbf_path"].exists()
    ]
    if not pbf_paths:
        logger.info("No PBF files found, skipping osm2pgsql")
        return

    logger.info("Importing %d PBF file(s) into PostGIS...", len(pbf_paths))
    env = os.environ.copy()
    env["PGPASSWORD"] = os.getenv("OSM_DB_PASSWORD", "")
    subprocess.run(
        [
            "osm2pgsql",
            "--output", "flex",
            "-S", str(PROJECT_ROOT / "osm2pgsql" / "generic.lua"),
            "-d", os.getenv("OSM_DB_NAME"),
            "-U", os.getenv("OSM_DB_USER"),
            "-H", os.getenv("OSM_DB_HOST"),
            "-P", os.getenv("OSM_DB_PORT"),
            *[str(p) for p in pbf_paths],
        ],
        check=True,
        env=env,
    )

    for p in pbf_paths:
        p.unlink()
    logger.info("osm2pgsql import complete (%d file(s))", len(pbf_paths))


def setup_mv_places():
    newest_ts = _newest_geofabrik_timestamp()
    conn = connect()
    try:
        last_date = last_import_date(conn, "osm")
        if last_date and last_date >= newest_ts:
            logger.info("OSM views already up-to-date (%s), skipping", last_date.date())
            record_import(conn, "osm", last_date, "skipped")
            return

        try:
            with conn.cursor() as cur:
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
                        geom
                    FROM polygons
                """)

                cur.execute("""
                    CREATE INDEX IF NOT EXISTS mv_places_geog_idx
                        ON mv_places USING GIST ((geom::geography));
                    CREATE INDEX IF NOT EXISTS mv_places_brand_wikidata_idx
                        ON mv_places ((brand_wikidata));
                    CREATE INDEX IF NOT EXISTS mv_places_brand_lower_idx
                        ON mv_places (LOWER(brand));
                    CREATE INDEX IF NOT EXISTS mv_places_name_lower_idx
                        ON mv_places (LOWER(name));
                    CREATE INDEX IF NOT EXISTS mv_places_website_norm_idx
                        ON mv_places (LOWER(REGEXP_REPLACE(website, '^https?://', '', 'i')));
                    CREATE INDEX IF NOT EXISTS mv_places_phone_norm_idx
                        ON mv_places (normalize_phone(phone));
                    CREATE INDEX IF NOT EXISTS mv_places_email_lower_idx
                        ON mv_places (LOWER(email));
                """)

            conn.commit()
            record_import(conn, "osm", newest_ts, "success")
            logger.info("mv_places created (data date: %s)", newest_ts.date())

        except Exception:
            logger.exception("setup_mv_places failed")
            try:
                record_import(conn, "osm", None, "error")
            except Exception:
                pass
            raise
    finally:
        conn.close()
