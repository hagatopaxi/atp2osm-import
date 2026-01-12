#!/usr/bin/env python3

import argparse
import duckdb
import logging
import requests
import sys
import os
import psycopg2

from utils import limit_offset, delete_file_if_exists, timer
from AtpPoi import AtpPoi
from OsmPoi import OsmPoi

logger = logging.getLogger(__name__)
osmdb = psycopg2.connect(
    dbname=os.getenv('OSM_DB_NAME'),
    user=os.getenv('OSM_DB_USER'),
    password=os.getenv('OSM_DB_PASSWORD'),
    host=os.getenv('OSM_DB_HOST'),
    port=os.getenv('OSM_DB_PORT')
)


class Config:
    args = None

    @staticmethod
    def setup(_args):
        Config.args = _args
    
    @staticmethod
    def debug():
        return Config.args.debug
    
    @staticmethod
    def brand():
        return Config.args.brand_wikidata

    @staticmethod
    def postcode():
        return Config.args.postcode

    @staticmethod
    def force_atp_setup():
        return Config.args.force_atp_setup

    @staticmethod
    def force_osm_setup():
        return Config.args.force_osm_setup


def download_latest_atp_data():
    if Config.force_atp_setup():
        logger.info("Forcing download of latest ATP data")
        delete_file_if_exists("./data/atp/latest.parquet")
        delete_file_if_exists("./data/atp/atp_fr.parquet")

    download_path = "./data/atp/latest.parquet"

    # If the download_path file is already there, skip the download
    if os.path.exists(download_path):
        logger.info(f"{download_path} already exists, skipping download")
        return

    url = "https://data.alltheplaces.xyz/runs/latest.json"
    response = requests.get(url)
    if response.status_code != 200:
        logger.error(f"Failed to download {url}")
        sys.exit(1)

    data = response.json()
    parquet_url = data.get("parquet_url")
    # If the download_path directory doesn't exist, create it
    os.makedirs(os.path.dirname(download_path), exist_ok=True)

    if not parquet_url:
        logger.error("'parquet_url' key not found in JSON response")
        sys.exit(1)

    logger.info("Downloading latest ATP data")
    parquet_response = requests.get(parquet_url)
    if parquet_response.status_code != 200:
        logger.error(f"Failed to download {parquet_url}")
        sys.exit(1)

    with open(download_path, 'wb') as file:
        file.write(parquet_response.content)

    logger.info(f"Downloaded {download_path}")


@timer
def setup_atp_fr_db():
    download_latest_atp_data()

    duckdb.sql("INSTALL spatial; LOAD spatial;")

    if os.path.exists('./data/atp/atp_fr.parquet'):
        logger.info("Loading existing atp_fr.parquet")
        duckdb.read_parquet('./data/atp/atp_fr.parquet')
        duckdb.sql("CREATE TABLE atp_fr AS SELECT * FROM 'data/atp/atp_fr.parquet'")
        return
    
    logger.info("Creating new atp_fr table and saving to parquet")
    duckdb.sql("""
        CREATE TABLE atp_fr AS
        SELECT
            properties->>'$.addr:country' as country,
            properties->>'$.addr:city' as city,
            properties->>'$.addr:postcode' as postcode,
            properties->>'$.brand:wikidata' as brand_wikidata,
            properties->>'$.brand' as brand,
            properties->>'$.name' as name,
            properties->>'$.opening_hours' as opening_hours,
            properties->>'$.website' as website,
            properties->>'$.phone' as phone,
            properties->>'$.email' as email,
            ST_AsGeoJSON(geom)
        FROM read_parquet('./data/atp/latest.parquet')
        WHERE properties->>'$.addr:country' = 'FR';
        CREATE INDEX atp_fr_brand_wikidata_idx ON atp_fr (brand_wikidata);
    """)
    duckdb.sql("COPY atp_fr TO './data/atp/atp_fr.parquet' (FORMAT parquet);")


@timer
def setup_osm_db():
    # Check if the extension pg_trgm is installed on the osm database
    cursor = osmdb.cursor()
    cursor.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm;")
    
    # Insert the EPSG/9793 official projection of France
    # See https://spatialreference.org/ref/epsg/9794/ and https://fr.wikipedia.org/wiki/Projection_conique_conforme_de_Lambert#Projections_officielles_en_France_m%C3%A9tropolitaine
    cursor.execute("SELECT * FROM spatial_ref_sys WHERE srid=9794;")
    spatial_refs = cursor.fetchall() 
    if len(spatial_refs) == 0:
        logger.info("Insert EPSG/9794 projection into OSM database")
        cursor.execute("""
            INSERT INTO spatial_ref_sys (srid, auth_name, auth_srid, srtext, proj4text) 
            VALUES(9794, 'EPSG', 9794, 'PROJCS["RGF93_v2b_Lambert-93",GEOGCS["RGF93_v2b",DATUM["Reseau_Geodesique_Francais_1993_v2b",SPHEROID["GRS_1980",6378137.0,298.257222101]],PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]],PROJECTION["Lambert_Conformal_Conic"],PARAMETER["False_Easting",700000.0],PARAMETER["False_Northing",6600000.0],PARAMETER["Central_Meridian",3.0],PARAMETER["Standard_Parallel_1",49.0],PARAMETER["Standard_Parallel_2",44.0],PARAMETER["Latitude_Of_Origin",46.5],UNIT["Meter",1.0]]', '+proj=lcc +lat_0=46.5 +lon_0=3 +lat_1=49 +lat_2=44 +x_0=700000 +y_0=6600000 +ellps=GRS80 +units=m +no_defs +type=crs');
        """)

    if Config.force_osm_setup():
        logger.info("Force OSM setup")
        cursor.execute("DROP MATERIALIZED VIEW IF EXISTS mv_places CASCADE;")

    logger.info("Create Materialized View mv_places and associated indexes")
    cursor.execute("""
        CREATE MATERIALIZED VIEW IF NOT EXISTS mv_places AS
        SELECT
            node_id                  AS osm_id,
            'point'                  AS node_type,
            tags->>'name'            AS name,
            tags->>'brand:wikidata'  AS brand_wikidata,
            tags->>'brand'           AS brand,
            tags->>'addr:city'       AS city,
            tags->>'addr:postcode'   AS postcode,
            tags->>'opening_hours'   AS opening_hours,
            tags->>'website'         AS website,
            tags->>'phone'           AS phone,
            tags->>'email'           AS email,
            ST_Transform(geom, 9794) AS geom_9794,
            geom
        FROM points

        UNION ALL

        SELECT
            area_id                  AS osm_id,
            'polygon'                AS node_type,
            tags->>'name'            AS name,
            tags->>'brand:wikidata'  AS brand_wikidata,
            tags->>'brand'           AS brand,
            tags->>'addr:city'       AS city,
            tags->>'addr:postcode'   AS postcode,
            tags->>'opening_hours'   AS opening_hours,
            tags->>'website'         AS website,
            tags->>'phone'           AS phone,
            tags->>'email'           AS email,
            ST_Transform(geom, 9794) AS geom_9794,
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

        -- 3.5  Index trigramme pour la similarité sur le nom
        CREATE INDEX IF NOT EXISTS mv_places_name_trgm_idx
            ON mv_places USING GIN (LOWER(name) gin_trgm_ops);

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

    cursor.close()
    logger.info("OSM DB completely setup")


def get_osm_poi(atp_poi: AtpPoi, i: int):
    try:
        # Create a cursor for each POI request
        cursor = osmdb.cursor()

        # SQL query to match OSM POIs based on ATP data
        # This query uses multiple matching strategies:
        # 1. Exact match on brand:wikidata tag
        # 2. Name similarity and proximity matching
        # 3. Address matching (postcode and city)
        query = """
            SELECT
                osm_id,
                node_type,
                name,
                brand_wikidata,
                brand,
                city,
                postcode,
                opening_hours,
                website,
                phone,
                email,
                geom
            FROM mv_places
            WHERE
                -- Filter by proximity (within 500 meters)
                ST_DWithin(
                    geom_9794,
                    ST_Transform(ST_GeomFromGeoJSON(%s), 9794),
                    500
                )
                AND (
                    -- Match by brand:wikidata
                    brand_wikidata = %s
                    -- Or match by brand name
                    OR LOWER(brand) = LOWER(%s)
                    -- Or match by exact name
                    OR LOWER(name) = LOWER(%s)
                    -- Or match by similar name
                    OR similarity(LOWER(name), LOWER(%s)) > 0.6
                    -- Or match by exact website (less http[s]://)
                    OR LOWER(REGEXP_REPLACE(website, '^https?://', '', 'i')) = LOWER(REGEXP_REPLACE(%s, '^https?://', '', 'i'))
                    -- Or match by exact phone number (without +33 prefix, replaced by 0, if anywhere)
                    OR REGEXP_REPLACE(REGEXP_REPLACE(phone, '^\+33', '0'), '\s+', '', 'g') = REGEXP_REPLACE(REGEXP_REPLACE(%s, '^\+33', '0'), '\s+', '', 'g')
                    -- Or match the exact email address
                    OR LOWER(email) = LOWER(%s)
                )
            LIMIT 2; -- only 2, it's to verify there is only one match.
            -- %s
            -- %s
        """
        query_params = (
            atp_poi.geom,  # For ST_Distance
            atp_poi.brand_wikidata,  # For brand:wikidata exact match
            atp_poi.brand,  # For brand name match
            atp_poi.name,  # For name exact match
            atp_poi.name,  # For name exact match
            atp_poi.website,  # For website match
            atp_poi.phone,  # For phone match
            atp_poi.email,  # For email match
            atp_poi.postcode, # For debug
            atp_poi.city, # For debug
        )
        # Convert DuckDB geometry to WKT format for PostGIS
        # Assuming geom is in WKT format or needs conversion
        # wkt_geom = f"POINT({atp_poi.geom})" if isinstance(atp_poi.geom, str) else str(atp_poi.geom)

        # Save the query to a file for debugging
        if Config.debug():
            # Create the debug folder if it does not exist
            if not os.path.exists("./data/debug"):
                os.makedirs("./data/debug")
            with open(f"./data/debug/{atp_poi.brand_wikidata}-{i}.sql", "w") as f:
                f.write(cursor.mogrify(query, query_params).decode('utf-8'))

        # Execute query with parameters
        cursor.execute(query, query_params)

        osm_pois = cursor.fetchall()
        polygons_matched = [poi for poi in osm_pois if poi[1] == "polygon"]
        points_matched = [poi for poi in osm_pois if poi[1] == "point"]

        if len(polygons_matched) == 0 and len(points_matched) == 0:
            # The POI does not exist in OSM. TODO: create a quest in StreetComplete
            logger.debug("POI's doesn't exist in OSM")
            return

        if len(polygons_matched) > 1 or len(points_matched) > 1:
            # There is more than 1 result, the POI is skipped
            logger.debug("There is more than one result in OSM")
            return

        for _osm_poi in osm_pois:
            osm_poi = OsmPoi(_osm_poi)
            logger.info(f"POI exists in OSM as {osm_poi.node_type}")
            # Complete the OSM poi with the ATP data
            apply_changes(atp_poi, osm_poi)
            # upload in OSM with a changeset, see https://wiki.openstreetmap.org/wiki/API_v0.6#JSON_Format

        # Process the results as needed
        return osm_poi
    finally:
        # Close the cursor after the query is executed
        if cursor:
            cursor.close()


def apply_changes(atp_poi: AtpPoi, osm_poi: OsmPoi):
    logger.info(f"{atp_poi}")
    logger.info(f"{osm_poi}")


@timer
def compute_changes():
    brands = duckdb.sql("""
        SELECT brand_wikidata, count(*)
        FROM atp_fr
        WHERE brand_wikidata IS NOT NULL
        GROUP BY brand_wikidata
    """).fetchall()

    for brand in brands:
        brand_wikidata = brand[0]
        brand_count = brand[1]

        if Config.brand() is not None and Config.brand() != brand_wikidata:
            # Filter brands
            continue

        limit = 100
        i = 0
        logger.info(f"Processing {brand_wikidata} with {brand_count} POIs")
        for skip in range(0, brand_count, 100):
            logger.info(f"Processing {brand_wikidata} {skip} to {min(skip + limit, brand_count)}")

            query_params = [brand_wikidata,]
            where_clause = ""
            if Config.postcode() is not None:
                query_params.append(Config.postcode())
                where_clause = " AND postcode = ?"

            atp_pois = duckdb.execute(f"""
                SELECT *
                FROM atp_fr
                WHERE brand_wikidata = ? {where_clause}
                LIMIT ? OFFSET ?
            """, query_params + [limit, skip]).fetchall()
            
            # Iterate on each value to get the OSM POIs
            for atp_poi in atp_pois:
                get_osm_poi(AtpPoi(atp_poi), i)
                i+=1


@timer
def main():
    parser = argparse.ArgumentParser(prog="atp2osm-import" ,description="Display CLI arguments")
    parser.add_argument("-d", "--debug", action="store_true", help="Enable debug mode")
    parser.add_argument("-b", "--brand-wikidata", action="store", help="Brand wikidata filter")
    parser.add_argument("-p", "--postcode", action="store", help="Postcode filter")
    parser.add_argument("--force-atp-setup", action="store_true", help="Force download and setup the latest ATP data")
    parser.add_argument("--force-osm-setup", action="store_true", help="Force setup the OSM database")

    args = parser.parse_args()
    Config.setup(args)

    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG if Config.debug() else logging.INFO)

    # 1. Setup ATP_FR table (download and extract)
    setup_atp_fr_db()

    # 2. Setup OSM database (create a view and necessary indexes)
    setup_osm_db()

    # 3. For each brands, check if there is an existing POI in OSM, then apply the changes
    compute_changes()


if __name__ == "__main__":
    main()

    # Close the osmdb connection at to end
    if osmdb:
        osmdb.close()
