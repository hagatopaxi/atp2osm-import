import logging
import requests
import os
import duckdb
import json

from datetime import datetime
from src.utils import delete_file_if_exists, timer, download_large_file
from pathlib import Path


logger = logging.getLogger(__name__)


def _get_last_import_date(osmdb, import_type):
    with osmdb.cursor() as cursor:
        cursor.execute(
            "SELECT date FROM data_imports WHERE type = %s ORDER BY date DESC LIMIT 1",
            (import_type,),
        )
        row = cursor.fetchone()
        return row[0] if row else None


def _record_import(osmdb, import_type, date):
    with osmdb.cursor() as cursor:
        cursor.execute(
            "INSERT INTO data_imports (type, date) VALUES (%s, %s)",
            (import_type, date),
        )
        osmdb.commit()


@timer
def import_atp_data(osmdb):
    try:
        last_date = _get_last_import_date(osmdb, "atp")

        response = requests.get("https://data.alltheplaces.xyz/runs/history.json", timeout=30)
        response.raise_for_status()
        runs = list(reversed(response.json()))

        atp_dir = Path("./data/atp")
        os.makedirs(atp_dir, exist_ok=True)
        parquet_path = atp_dir / "latest.parquet"
        stats_path = atp_dir / "stats.json"
        spiders_path = atp_dir / "spiders.json"

        used_end_time = None
        for run in runs:
            end_time_raw = run.get("end_time")
            end_time = (
                datetime.fromisoformat(end_time_raw.replace("Z", "+00:00"))
                if end_time_raw else None
            )
            parquet_url = run.get("parquet_url")
            stats_url = run.get("stats_url")
            run_id = run.get("run_id")

            if not parquet_url:
                continue
            if last_date is not None and end_time is not None and end_time <= last_date:
                logger.info(
                    f"Reached already-imported run {run_id} ({end_time.date()}), stopping"
                )
                return

            try:
                delete_file_if_exists(parquet_path)
                delete_file_if_exists(stats_path)
                download_large_file(parquet_url, parquet_path)
                with open(parquet_path, "rb") as f:
                    if f.read(4) != b"PAR1":
                        raise ValueError(f"Invalid parquet file for run {run_id}")
                if stats_url:
                    download_large_file(stats_url, stats_path)
                    with open(stats_path) as infile, open(spiders_path, "w") as out:
                        out.write(json.dumps(json.loads(infile.read())["results"]))
                    delete_file_if_exists(stats_path)
                used_end_time = end_time
                logger.info(f"Downloaded ATP run {run_id}")
                break
            except Exception as exc:
                logger.warning(f"Failed to download ATP run {run_id}: {exc}, trying previous...")
                delete_file_if_exists(parquet_path)
                delete_file_if_exists(stats_path)

        if used_end_time is None:
            raise RuntimeError("No ATP run could be downloaded")

        duckdb.sql("INSTALL postgres; LOAD postgres;")
        duckdb.sql("INSTALL spatial; LOAD spatial;")

        with osmdb.cursor() as cursor:
            cursor.execute("DROP TABLE IF EXISTS atp_fr CASCADE")
            osmdb.commit()

        duckdb.execute(
            f"ATTACH 'dbname={os.getenv('OSM_DB_NAME')} user={os.getenv('OSM_DB_USER')} host={os.getenv('OSM_DB_HOST')} password={os.getenv('OSM_DB_PASSWORD')} port={os.getenv('OSM_DB_PORT')}' AS pg (TYPE postgres);",
        )

        logger.info("Creating new atp_fr table from parquet file")
        duckdb.sql("""
            CREATE TABLE IF NOT EXISTS pg.atp_fr AS
            SELECT
                id,
                properties->>'$.addr:country' as country,
                properties->>'$.addr:city' as city,
                properties->>'$.addr:postcode' as postcode,
                TRY_CAST(SUBSTRING(properties->>'$.addr:postcode', 1, 2) AS INTEGER) as departement_number,
                properties->>'$.brand:wikidata' as brand_wikidata,
                properties->>'$.brand' as brand,
                properties->>'$.name' as name,
                properties->>'$.opening_hours' as opening_hours,
                properties->>'$.website' as website,
                properties->>'$.phone' as phone,
                properties->>'$.email' as email,
                properties->>'$.end_date' as end_date,
                dataset_attributes->>'$.@spider' as spider_id,
                dataset_attributes->>'$.source' as source_type,
                properties->>'$.@source_uri' as source_uri,
                ST_AsGeoJSON(geom) as geom
            FROM read_parquet('./data/atp/latest.parquet')
            WHERE properties->>'$.addr:country' = 'FR'
                AND map_extract(properties, 'addr:postcode') IS NOT NULL
                AND geom IS NOT NULL
                AND REGEXP_MATCHES(SUBSTRING(properties->>'$.addr:postcode', 1, 2), '^[0-9]+$') -- Remove postcode error
                AND TRY_CAST(SUBSTRING(properties->>'$.addr:postcode', 1, 2) AS INTEGER) BETWEEN 1 AND 95; -- Keep only metropolitan POIs
        """)

        logger.info("Creating indexes for atp_fr fields")
        with osmdb.cursor() as cursor:
            cursor.execute("DELETE FROM atp_fr WHERE postcode IS NULL;")
            cursor.execute("""
                -- 3.1  Index spatial (GIST) – indispensable pour ST_DWithin
                CREATE INDEX IF NOT EXISTS atp_fr_geom_idx
                    ON atp_fr USING GIST (ST_Transform(ST_GeomFromGeoJSON(geom), 9794));

                -- 3.2  Index sur la clé brand:wikidata (exact match)
                CREATE INDEX IF NOT EXISTS atp_fr_brand_wikidata_idx
                    ON atp_fr (brand_wikidata);

                -- 3.3  Index fonctionnel insensible à la casse sur brand
                CREATE INDEX IF NOT EXISTS atp_fr_brand_lower_idx
                    ON atp_fr (LOWER(brand));

                -- 3.4  Index fonctionnel insensible à la casse sur name
                CREATE INDEX IF NOT EXISTS atp_fr_name_lower_idx
                    ON atp_fr (LOWER(name));

                -- 3.6  Normalisation du site web (supprime http/https) – insensible à la casse
                CREATE INDEX IF NOT EXISTS atp_fr_website_norm_idx
                    ON atp_fr (LOWER(REGEXP_REPLACE(website, '^https?://', '', 'i')));

                -- 3.7  Normalisation du téléphone (supprime le préfixe +33 et les espaces)
                CREATE INDEX IF NOT EXISTS atp_fr_phone_norm_idx
                    ON atp_fr (REGEXP_REPLACE(REGEXP_REPLACE(phone, '^\+33', '0'), '\s+', '', 'g'));

                -- 3.8  Index fonctionnel insensible à la casse sur l'email
                CREATE INDEX IF NOT EXISTS atp_fr_email_lower_idx
                    ON atp_fr (LOWER(email));

                -- 3.9  Index fonctionnel insensible à la casse sur l'email
                CREATE INDEX IF NOT EXISTS atp_fr_departement_number_idx
                    ON atp_fr (departement_number);

                -- 3.10  Index fonctionnel sur spider_id
                CREATE INDEX IF NOT EXISTS atp_fr_spider_idx
                    ON atp_fr (spider_id);

                -- 3.11  Index fonctionnel sur source_type
                CREATE INDEX IF NOT EXISTS atp_fr_source_type_idx
                    ON atp_fr (source_type);
            """)
            osmdb.commit()

            logger.info("Creating new atp_spiders table from stats json and parquet data")
            duckdb.sql("""
                CREATE TABLE IF NOT EXISTS pg.atp_spiders AS
                SELECT *
                FROM read_json('./data/atp/spiders.json')
                WHERE spider IN (SELECT distinct(spider_id) FROM pg.atp_fr)
            """)

        if used_end_time is not None:
            _record_import(osmdb, "atp", used_end_time)
            logger.info(f"Recorded ATP import date: {used_end_time.date()}")

    except Exception:
        logger.exception("import_atp_data failed")
        raise


@timer
def import_osm_data(osmdb, skip_mv=False, osm_date=None):
    if skip_mv:
        logger.info("Skipping OSM setup (--skip-mv)")
        return

    try:
        with osmdb.cursor() as cursor:
            # Insert the EPSG/9794 official projection of France
            # See https://spatialreference.org/ref/epsg/9794/ and https://fr.wikipedia.org/wiki/Projection_conique_conforme_de_Lambert#Projections_officielles_en_France_m%C3%A9tropolitaine
            cursor.execute("SELECT * FROM spatial_ref_sys WHERE srid=9794;")
            spatial_refs = cursor.fetchall()
            if len(spatial_refs) == 0:
                logger.info("Insert EPSG/9794 projection into OSM database")
                cursor.execute("""
                    INSERT INTO spatial_ref_sys (srid, auth_name, auth_srid, srtext, proj4text)
                    VALUES(9794, 'EPSG', 9794, 'PROJCS["RGF93_v2b_Lambert-93",GEOGCS["RGF93_v2b",DATUM["Reseau_Geodesique_Francais_1993_v2b",SPHEROID["GRS_1980",6378137.0,298.257222101]],PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]],PROJECTION["Lambert_Conformal_Conic"],PARAMETER["False_Easting",700000.0],PARAMETER["False_Northing",6600000.0],PARAMETER["Central_Meridian",3.0],PARAMETER["Standard_Parallel_1",49.0],PARAMETER["Standard_Parallel_2",44.0],PARAMETER["Latitude_Of_Origin",46.5],UNIT["Meter",1.0]]', '+proj=lcc +lat_0=46.5 +lon_0=3 +lat_1=49 +lat_2=44 +x_0=700000 +y_0=6600000 +ellps=GRS80 +units=m +no_defs +type=crs');
                """)

        with osmdb.cursor() as cursor:
            cursor.execute("DROP MATERIALIZED VIEW IF EXISTS mv_places CASCADE;")

            logger.info("Create Materialized View mv_places and associated indexes")
            cursor.execute("""
                CREATE MATERIALIZED VIEW IF NOT EXISTS mv_places AS
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
                    ST_Transform(geom, 9794)                             AS geom_9794,
                    geom
                FROM points

                UNION ALL

                SELECT
                    area_id                                              AS osm_id,
                    'relation'                                           AS node_type,
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
                    ST_Transform(geom, 9794)                             AS geom_9794,
                    geom
                FROM polygons;

                -- 3.1  Index spatial (GIST) – indispensable pour ST_DWithin
                CREATE INDEX IF NOT EXISTS mv_places_geom_9794_idx
                    ON mv_places USING GIST (geom_9794);

                -- 3.2  Index sur la clé brand:wikidata (exact match)
                CREATE INDEX IF NOT EXISTS mv_places_brand_wikidata_idx
                    ON mv_places ((brand_wikidata));

                -- 3.3  Index fonctionnel insensible à la casse sur brand
                CREATE INDEX IF NOT EXISTS mv_places_brand_lower_idx
                    ON mv_places (LOWER(brand));

                -- 3.4  Index fonctionnel insensible à la casse sur name
                CREATE INDEX IF NOT EXISTS mv_places_name_lower_idx
                    ON mv_places (LOWER(name));

                -- 3.6  Normalisation du site web (supprime http/https) – insensible à la casse
                CREATE INDEX IF NOT EXISTS mv_places_website_norm_idx
                    ON mv_places (LOWER(REGEXP_REPLACE(website, '^https?://', '', 'i')));

                -- 3.7  Normalisation du téléphone (supprime le préfixe +33 et les espaces)
                CREATE INDEX IF NOT EXISTS mv_places_phone_norm_idx
                    ON mv_places (REGEXP_REPLACE(REGEXP_REPLACE(phone, '^\+33', '0'), '\s+', '', 'g'));

                -- 3.8  Index fonctionnel insensible à la casse sur l'email
                CREATE INDEX IF NOT EXISTS mv_places_email_lower_idx
                    ON mv_places (LOWER(email));
            """)

            osmdb.commit()

        if osm_date is not None:
            _record_import(osmdb, "osm", osm_date)
            logger.info(f"OSM DB completely setup (data date: {osm_date.date()})")
        else:
            logger.info("OSM DB completely setup")

    except Exception:
        logger.exception("import_osm_data failed")
        raise


@timer
def setup_atp2osm_db(osmdb, skip_mv=False, osm_date=None):
    import_osm_data(osmdb, skip_mv=skip_mv, osm_date=osm_date)
    import_atp_data(osmdb)
