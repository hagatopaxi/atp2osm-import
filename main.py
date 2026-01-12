#!/usr/bin/env python3

import argparse
import duckdb
import logging
import requests
import sys
import os
import psycopg2

from utils import limit_offset
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


def download_latest_atp_data():
    url = "https://data.alltheplaces.xyz/runs/latest.json"
    response = requests.get(url)
    if response.status_code != 200:
        logger.error(f"  Failed to download {url}")
        sys.exit(1)

    data = response.json()
    parquet_url = data.get("parquet_url")
    download_path = "./data/atp/" + data.get("run_id") + '.parquet'
    # If the download_path directory doesn't exist, create it
    os.makedirs(os.path.dirname(download_path), exist_ok=True)

    # If the download_path file is already there, skip the download
    if os.path.exists(download_path):
        logger.info(f"{download_path} already exists, skipping download")
        return download_path

    if not parquet_url:
        logger.error("'parquet_url' key not found in JSON response")
        sys.exit(1)

    parquet_response = requests.get(parquet_url)
    if parquet_response.status_code != 200:
        logger.error(f"Failed to download {parquet_url}")
        sys.exit(1)

    with open(download_path, 'wb') as file:
        file.write(parquet_response.content)

    logger.info(f"Downloaded {download_path}")
    return download_path


def setup_atp_fr_db(parquet_path: str):
    duckdb.sql("INSTALL spatial; LOAD spatial;")

    if os.path.exists('./data/atp/atp_fr.parquet'):
        logger.info("Loading existing atp_fr.parquet")
        duckdb.read_parquet('./data/atp/atp_fr.parquet')
        duckdb.sql("CREATE TABLE atp_fr AS SELECT * FROM 'data/atp/atp_fr.parquet'")
        return
    
    logger.info("Creating new atp_fr table and saving to parquet")
    duckdb.read_parquet(parquet_path)
    duckdb.sql(f"""
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
        FROM '{parquet_path}'
        WHERE properties->>'$.addr:country' = 'FR';
        CREATE INDEX atp_fr_brand_wikidata_idx ON atp_fr (brand_wikidata);
    """)
    duckdb.sql("COPY atp_fr TO './data/atp/atp_fr.parquet' (FORMAT parquet);")


def setup_osm_db():
    # Check if the extension pg_trgm is installed on the osm database
    cursor = osmdb.cursor()
    cursor.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm;")
    
    # Insert the EPSG/9793 official projection of France
    # See https://spatialreference.org/ref/epsg/9794/ and https://fr.wikipedia.org/wiki/Projection_conique_conforme_de_Lambert#Projections_officielles_en_France_m%C3%A9tropolitaine
    cursor.execute("SELECT * FROM spatial_ref_sys WHERE srid=9794;")
    spatial_refs = cursor.fetchall() 
    if len(spatial_refs) == 0:
        cursor.execute("""
            INSERT INTO spatial_ref_sys (srid, auth_name, auth_srid, srtext, proj4text) 
            VALUES(9794, 'EPSG', 9794, 'PROJCS["RGF93_v2b_Lambert-93",GEOGCS["RGF93_v2b",DATUM["Reseau_Geodesique_Francais_1993_v2b",SPHEROID["GRS_1980",6378137.0,298.257222101]],PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]],PROJECTION["Lambert_Conformal_Conic"],PARAMETER["False_Easting",700000.0],PARAMETER["False_Northing",6600000.0],PARAMETER["Central_Meridian",3.0],PARAMETER["Standard_Parallel_1",49.0],PARAMETER["Standard_Parallel_2",44.0],PARAMETER["Latitude_Of_Origin",46.5],UNIT["Meter",1.0]]', '+proj=lcc +lat_0=46.5 +lon_0=3 +lat_1=49 +lat_2=44 +x_0=700000 +y_0=6600000 +ellps=GRS80 +units=m +no_defs +type=crs');
        """)

    cursor.close()


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
        (SELECT
            node_id as osm_id,
            'point' as node_type,
            tags->'name' as name,
            tags->'brand:wikidata' as brand_wikidata,
            tags->'brand' as brand,
            tags->'addr:city' as city,
            tags->'addr:postcode' as postcode,
            tags->'opening_hours' as opening_hours,
            tags->'website' as website,
            tags->'phone' as phone,
            tags->'email' as email,
            geom
        FROM points
        WHERE
            -- Filter by proximity (within 500 meters)
            ST_DWithin(
                ST_Transform(geom, 9794),
                ST_Transform(ST_GeomFromGeoJSON(%s), 9794),
                500
            )
            AND (
                -- Match by brand:wikidata
                tags->>'brand:wikidata' = %s
                -- Or match by brand name
                OR LOWER(tags->>'brand') = LOWER(%s)
                -- Or match by exact name
                OR LOWER(tags->>'name') = LOWER(%s)
                -- Or match by similar name
                OR similarity(LOWER(tags->>'name'), LOWER(%s)) > 0.6
                -- Or match by exact website (less http[s]://)
                OR LOWER(REGEXP_REPLACE(tags->>'website', '^https?://', '')) = LOWER(REGEXP_REPLACE(%s, '^https?://', ''))
                -- Or match by exact phone number (without +33 prefix, replaced by 0, if anywhere)
                OR REGEXP_REPLACE(REGEXP_REPLACE(tags->>'phone', '^\+33', '0'), ' ', '') = REGEXP_REPLACE(%s, '^\+33', '0')
                -- Or match the exact email address
                OR LOWER(tags->>'email') = LOWER(%s)
            )
        LIMIT 2) -- only 2, it's to verify there is only one match.
        UNION ALL
        (SELECT
            area_id as osm_id,
            'polygon' as node_type,
            tags->'name' as name,
            tags->'brand:wikidata' as brand_wikidata,
            tags->'brand' as brand,
            tags->'addr:city' as city,
            tags->'addr:postcode' as postcode,
            tags->'opening_hours' as opening_hours,
            tags->'website' as website,
            tags->'phone' as phone,
            tags->'email' as email,
            geom
        FROM polygons
        WHERE
            -- Filter by proximity (within 500 meters)
            ST_DWithin(
                ST_Transform(geom, 9794),
                ST_Transform(ST_GeomFromGeoJSON(%s), 9794),
                500
            )
            AND (
                -- Match by brand:wikidata
                tags->>'brand:wikidata' = %s
                -- Or match by brand name
                OR LOWER(tags->>'brand') = LOWER(%s)
                -- Or match by exact name
                OR LOWER(tags->>'name') = LOWER(%s)
                -- Or match by similar name
                OR similarity(LOWER(tags->>'name'), LOWER(%s)) > 0.6
                -- Or match by exact website (less http[s]://)
                OR LOWER(REGEXP_REPLACE(tags->>'website', '^https?://', '')) = LOWER(REGEXP_REPLACE(%s, '^https?://', ''))
                -- Or match by exact phone number (without +33 prefix, replaced by 0, if anywhere)
                OR REGEXP_REPLACE(REGEXP_REPLACE(tags->>'phone', '^\+33', '0'), ' ', '') = REGEXP_REPLACE(%s, '^\+33', '0')
                -- Or match the exact email address
                OR LOWER(tags->>'email') = LOWER(%s)
            )
        LIMIT 2); -- only 2, it's to verify there is only one match.
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


def compute_changes(brands):
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
            logger.debug(f"Processing {brand_wikidata} {skip} to {min(skip + limit, brand_count)}")

            where_clause = f" AND postcode = {Config.postcode()}" if Config.postcode() is not None else ""

            atp_pois = duckdb.sql(f"""
                SELECT *
                FROM atp_fr
                WHERE brand_wikidata = '{brand_wikidata}' {where_clause}
                LIMIT {limit} OFFSET {skip}
            """).fetchall()
            
            # Iterate on each value to get the OSM POIs
            for atp_poi in atp_pois:
                get_osm_poi(AtpPoi(atp_poi), i)
                i+=1


def main():
    parser = argparse.ArgumentParser(prog="atp2osm-import" ,description="Display CLI arguments")
    parser.add_argument("-d", "--debug", action="store_true", help="Enable debug mode")
    parser.add_argument("-b", "--brand-wikidata", action="store", help="Brand wikidata filter")
    parser.add_argument("-p", "--postcode", action="store", help="Postcode filter")

    args = parser.parse_args()
    Config.setup(args)

    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG if Config.debug() else logging.INFO)

    # 1. Download the ATP data
    latest_parquet_path = download_latest_atp_data()

    # 2. Get every ATP POI's which is located in the France area territory
    setup_atp_fr_db(latest_parquet_path)
    setup_osm_db()
    brands = duckdb.sql("""
        SELECT brand_wikidata, count(*)
        FROM atp_fr
        WHERE brand_wikidata IS NOT NULL
        GROUP BY brand_wikidata
    """).fetchall()
    brands_count = duckdb.sql("""
        SELECT count(DISTINCT brand_wikidata)
        FROM atp_fr
        WHERE brand_wikidata IS NOT NULL
    """) # 421

    # 3. Based on this data, loop on every brand and items to find a associated OSM POI's
    compute_changes(brands)

    # 4. If the OSM POI is not found skip for now

    # 5. If the OSM POI is found, complete the data (phone, website, opening_hours, email_address) with the ATP POI value

    # 6. Save the changes in a .osc file

    # 7. Publish the file to OSM


if __name__ == "__main__":
    main()

    # Close the osmdb connection at to end
    if osmdb:
        osmdb.close()
