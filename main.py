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

logger = logging.getLogger(__name__)
osmdb = psycopg2.connect(
    dbname=os.getenv('OSM_DB_NAME'),
    user=os.getenv('OSM_DB_USER'),
    password=os.getenv('OSM_DB_PASSWORD'),
    host=os.getenv('OSM_DB_HOST'),
    port=os.getenv('OSM_DB_PORT')
)

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
        logger.info(f"  {download_path} already exists, skipping download")
        return download_path

    if not parquet_url:
        logger.error("  'parquet_url' key not found in JSON response")
        sys.exit(1)

    parquet_response = requests.get(parquet_url)
    if parquet_response.status_code != 200:
        logger.error(f"  Failed to download {parquet_url}")
        sys.exit(1)

    with open(download_path, 'wb') as file:
        file.write(parquet_response.content)

    logger.info(f"  Downloaded {download_path}")
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
            geom
        FROM '{parquet_path}'
        WHERE properties->>'$.addr:country' = 'FR';
        CREATE INDEX atp_fr_brand_wikidata_idx ON atp_fr (brand_wikidata);
    """)
    duckdb.sql("COPY atp_fr TO './data/atp/atp_fr.parquet' (FORMAT parquet);")

def get_osm_pois(atp_poi: AtpPoi):
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
            node_id,
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
                geom::geography,
                ST_GeomFromText(%s, 4326)::geography,
                500
            )
            AND (
                -- Match by brand:wikidata
                tags->'brand:wikidata' = %s
                -- Or match by brand name
                OR LOWER(tags->'brand') = LOWER(%s)
                -- Or match by similar name
                OR similarity(LOWER(name), LOWER(%s)) > 0.6
                -- Or match by exact name
                OR LOWER(name) = LOWER(%s)
                -- Or match by address
                OR (tags->'addr:postcode' = %s AND LOWER(tags->'addr:city') = LOWER(%s))
                -- Or match by exact website (less http[s]://)
                OR LOWER(REGEXP_REPLACE(tags->'website', '^https?://', '')) = LOWER(REGEXP_REPLACE(%s, '^https?://', ''))
                -- Or match by exact phone number (without +33 prefix, replaced by 0, if anywhere)
                OR tags->REGEXP_REPLACE(REGEXP_REPLACE('phone', '^+33', '0'), ' ', '') = REGEXP_REPLACE(%s, '^+33', '0')
                -- Or match the exact email address
                OR LOWER(tags->'email') = LOWER(%s)
            )
        LIMIT 2; -- only 2, it's to verify there is only one match.
        """

        # Convert DuckDB geometry to WKT format for PostGIS
        # Assuming geom is in WKT format or needs conversion
        wkt_geom = f"POINT({atp_poi.geom})" if isinstance(atp_poi.geom, str) else str(atp_poi.geom)

        # Save the query to a file for debugging
        with open(f"./data/debug/{atp_poi.brand_wikidata}.sql", "w") as f:
            logger.info(cursor.morgify(query, (
                wkt_geom,  # For ST_Distance
                atp_poi.brand_wikidata,  # For brand:wikidata exact match
                atp_poi.brand,  # For brand name match
                atp_poi.name,  # For name exact match
                atp_poi.name,  # For name exact match
                atp_poi.postcode,  # For postcode match
                atp_poi.city,  # For city match
                atp_poi.website,  # For website match
                atp_poi.phone,  # For phone match
                atp_poi.email,  # For email match
            )))
            f.write(cursor.morgify(query, (
                wkt_geom,  # For ST_Distance
                atp_poi.brand_wikidata,  # For brand:wikidata exact match
                atp_poi.brand,  # For brand name match
                atp_poi.name,  # For name exact match
                atp_poi.name,  # For name exact match
                atp_poi.postcode,  # For postcode match
                atp_poi.city,  # For city match
                atp_poi.website,  # For website match
                atp_poi.phone,  # For phone match
                atp_poi.email,  # For email match
            )))

        # Execute query with parameters
        cursor.execute(query, (
            wkt_geom,  # For ST_Distance
            atp_poi.brand_wikidata,  # For brand:wikidata exact match
            atp_poi.brand,  # For brand name match
            atp_poi.name,  # For name exact match
            atp_poi.name,  # For name exact match
            atp_poi.postcode,  # For postcode match
            atp_poi.city,  # For city match
            atp_poi.website,  # For website match
            atp_poi.phone,  # For phone match
            atp_poi.email,  # For email match
        ))

        osm_pois = cursor.fetchall()

        if len(osm_pois) == 0:
            # The POI does not exist in OSM. TODO: create a quest in StreetComplete
            logger.info("POI's doesn't exist in OSM")
            return

        if len(osm_pois) > 1:
            # There is more than 1 result, the POI is skipped
            logger.info("There is more than one result in OSM")
            return

        osm_poi = osm_pois[0]
        # Complete the OSM poi with the ATP data
        # osm_poi = complete_osm_poi(osm_poi, atp_poi)
        # upload in OSM with a changeset, see https://wiki.openstreetmap.org/wiki/API_v0.6#JSON_Format

        # Process the results as needed
        return pois
    finally:
        # Close the cursor after the query is executed
        if cursor:
            cursor.close()

def compute_changes(brands):
    for brand in brands:
        brand_wikidata = brand[0]
        brand_count = brand[1]
        limit = 100
        logger.info(f"Processing {brand_wikidata} with {brand_count} POIs")
        for skip in range(0, brand_count, 100):
            logger.debug(f"Processing {brand_wikidata} {skip} to {min(skip + limit, brand_count)}")
            atp_pois = duckdb.sql(f"""
                SELECT *
                FROM atp_fr
                WHERE brand_wikidata = '{brand_wikidata}'
                LIMIT {limit} OFFSET {skip}
            """).fetchall()

            # Iterate on each value to get the OSM POIs
            for atp_poi in atp_pois:
                osm_pois = get_osm_pois(AtpPoi(atp_poi))

def main():
    parser = argparse.ArgumentParser(prog="atp2osm-import" ,description="Display CLI arguments")

    logging.basicConfig(stream=sys.stdout, level=logging.INFO)
    args = parser.parse_args()

    # 1. Download the ATP data
    latest_parquet_path = download_latest_atp_data()

    # 2. Get every ATP POI's which is located in the France area territory
    setup_atp_fr_db(latest_parquet_path)
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
