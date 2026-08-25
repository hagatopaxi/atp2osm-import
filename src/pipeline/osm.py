import logging
import os
import shutil
import subprocess
from datetime import datetime
from email.utils import parsedate_to_datetime
from src.pipeline import _matview
from src.pipeline.constants import (
    PROJECT_ROOT,
    GEOFABRIK_REGIONS,
)

import requests

from src.config import get_database, get_pipeline
from src.pipeline._db import (
    connect,
    last_import_comment,
    last_import_date,
    record_import,
    start_import,
)
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
            logger.error("Could not fetch timestamp for %s: %s", name, exc)
            raise
    if not timestamps:
        raise RuntimeError("No Geofabrik timestamps could be fetched")
    return max(timestamps)


def download_pbf():
    newest_ts = _newest_geofabrik_timestamp()

    conn = connect()
    try:
        last_date = last_import_date(conn, "osm")
        start_import(conn, "osm")  # puts the site in maintenance mode

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


def _require_free_space(path, needed_bytes):
    """Fast-fail if the filesystem holding `path` has less than `needed_bytes` free.

    osm2pgsql --create drops and recreates points/polygons with CASCADE, which
    also drops mv_places and mv_places_brand. If it then runs out of disk it
    exits non-zero with the views already gone — leaving the site broken. Bail
    out *before* that happens, with a clear message.
    """
    free = shutil.disk_usage(path).free
    if free < needed_bytes:
        raise RuntimeError(
            f"Not enough free disk for osm2pgsql: {free / 1e9:.1f} GB free at "
            f"{path}, need ~{needed_bytes / 1e9:.1f} GB. Free space and retry "
            f"(`python -m src.pipeline from osm-import`)."
        )


def run_osm2pgsql():
    pbf_paths = [
        r["pbf_path"]
        for r in GEOFABRIK_REGIONS.values()
        if r["pbf_path"].exists()
    ]
    if not pbf_paths:
        logger.info("No PBF files found, skipping osm2pgsql")
        return

    # Fast-fail on low disk before the destructive CASCADE-dropping import.
    # Heuristic: need ~3x total PBF size (tables + indexes + temp), floor 15 GB.
    # Override the floor with OSM2PGSQL_MIN_FREE_GB.
    total_pbf = sum(p.stat().st_size for p in pbf_paths)
    floor = get_pipeline().min_free_gb * 1e9
    needed = max(floor, 3 * total_pbf)
    _require_free_space(pbf_paths[0].parent, needed)

    db = get_database()
    logger.info("Importing %d PBF file(s) into PostGIS...", len(pbf_paths))
    env = os.environ.copy()
    env["PGPASSWORD"] = db.password
    subprocess.run(
        [
            "osm2pgsql",
            "--output", "flex",
            "-S", str(PROJECT_ROOT / "osm2pgsql" / "generic.lua"),
            "-d", db.name,
            "-U", db.user,
            "-H", db.host,
            "-P", db.port,
            *[str(p) for p in pbf_paths],
        ],
        check=True,
        env=env,
    )

    for p in pbf_paths:
        p.unlink()
    logger.info("osm2pgsql import complete (%d file(s))", len(pbf_paths))


def _mv_places_sql() -> str:
    # Only rows that can ever match are kept: the join in
    # MATCHED_POI_SQL requires an equality on one of brand:wikidata,
    # brand, name, email, website or phone, and NULL never equals
    # anything. That drops 95% of the OSM objects (20.3M -> 1.1M) and
    # cuts the /validate query time by a third to two thirds, with a
    # provably identical result.
    matchable = """
        WHERE tags ?| ARRAY['name', 'brand', 'brand:wikidata',
                            'email', 'contact:email',
                            'phone', 'contact:phone',
                            'website', 'contact:website']
    """
    # The matchable filter sits in a subquery so that the NSI
    # lookup only ever runs on the 1.1M rows that survive it,
    # never on the 20.3M raw ones.
    return f"""
        CREATE MATERIALIZED VIEW mv_places AS
        SELECT
            node_id                                              AS osm_id,
            'node'                                               AS node_type,
            points.tags                                          AS tags,
            points.tags->>'name'                                 AS name,
            COALESCE(points.tags->>'brand:wikidata',
                     nsi.tags->>'brand:wikidata')        AS brand_wikidata,
            CASE WHEN points.tags ? 'brand:wikidata' THEN 'osm'
                 WHEN nsi.tags IS NOT NULL          THEN 'nsi'
            END                                                  AS brand_wikidata_source,
            nsi.tags                                             AS nsi_tags,
            points.tags->>'brand'                                AS brand,
            points.tags->>'addr:city'                            AS city,
            points.tags->>'addr:postcode'                        AS postcode,
            points.tags->>'opening_hours'                        AS opening_hours,
            COALESCE(points.tags->>'website', points.tags->>'contact:website') AS website,
            COALESCE(points.tags->>'phone', points.tags->>'contact:phone')     AS phone,
            COALESCE(points.tags->>'email', points.tags->>'contact:email')     AS email,
            version,
            NULL::jsonb                                          AS members,
            geom
        FROM (SELECT * FROM points {matchable}) points
        LEFT JOIN LATERAL nsi_match(points.tags) AS nsi(tags) ON TRUE

        UNION ALL

        SELECT
            area_id                                              AS osm_id,
            CASE osm_type WHEN 'W' THEN 'way' ELSE 'relation' END AS node_type,
            polygons.tags                                        AS tags,
            polygons.tags->>'name'                               AS name,
            COALESCE(polygons.tags->>'brand:wikidata',
                     nsi.tags->>'brand:wikidata')        AS brand_wikidata,
            CASE WHEN polygons.tags ? 'brand:wikidata' THEN 'osm'
                 WHEN nsi.tags IS NOT NULL          THEN 'nsi'
            END                                                  AS brand_wikidata_source,
            nsi.tags                                             AS nsi_tags,
            polygons.tags->>'brand'                              AS brand,
            polygons.tags->>'addr:city'                          AS city,
            polygons.tags->>'addr:postcode'                      AS postcode,
            polygons.tags->>'opening_hours'                      AS opening_hours,
            COALESCE(polygons.tags->>'website', polygons.tags->>'contact:website') AS website,
            COALESCE(polygons.tags->>'phone', polygons.tags->>'contact:phone')     AS phone,
            COALESCE(polygons.tags->>'email', polygons.tags->>'contact:email')     AS email,
            version,
            members                                              AS members,
            geom
        FROM (SELECT * FROM polygons {matchable}) polygons
        LEFT JOIN LATERAL nsi_match(polygons.tags) AS nsi(tags) ON TRUE
    """


def setup_mv_places():
    view_sql = _mv_places_sql()
    newest_ts = _newest_geofabrik_timestamp()
    conn = connect()
    try:
        last_date = last_import_date(conn, "osm")
        # mv_places reads the OSM tables and nsi_brands: a new NSI release must
        # rebuild it even when the OSM data has not moved. NSI is identified by
        # its published version rather than a date — that is what names the
        # content, and two releases can share a day.
        #
        # nsi_tags is computed by nsi_match() and stored, so a migration that
        # redefines the function is an input too: without it, the corrected
        # rows only land the next time the OSM data happens to move.
        signature = _matview.signature(
            view_sql,
            last_import_comment(conn, "nsi"),
            _matview.function_defs(conn, "nsi_match", "osm_primary_tag"),
        )
        if (
            last_date
            and last_date >= newest_ts
            and _matview.is_current(conn, "mv_places", signature)
        ):
            logger.info("OSM views already up-to-date (%s), skipping", last_date.date())
            record_import(conn, "osm", last_date, "skipped")
            return

        try:
            with conn.cursor() as cur:
                cur.execute("DROP MATERIALIZED VIEW IF EXISTS mv_places CASCADE;")
                logger.info("Creating mv_places and indexes...")
                cur.execute(view_sql)

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

                _matview.stamp(cur, "mv_places", signature)

            conn.commit()
            record_import(conn, "osm", newest_ts, "success")
            logger.info("mv_places created (data date: %s)", newest_ts.date())

        except Exception:
            logger.exception("setup_mv_places failed")
            raise
    finally:
        conn.close()
