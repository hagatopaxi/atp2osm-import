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
    duckdb.read_parquet(parquet_path)
    duckdb.sql("INSTALL spatial; LOAD spatial;")
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
    # logger.info(duckdb.sql(f"SELECT * FROM '{parquet_path}' LIMIT 10"))
    # logger.info(duckdb.sql("SELECT * FROM atp_fr LIMIT 10"))


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
            osm_id,
            name,
            tags->>'brand:wikidata' as brand_wikidata,
            tags->>'brand' as brand,
            tags->>'addr:city' as city,
            tags->>'addr:postcode' as postcode,
            tags->>'opening_hours' as opening_hours,
            tags->>'website' as website,
            tags->>'phone' as phone,
            tags->>'email' as email,
            way,
            ST_Distance(
                way::geography,
                ST_GeomFromText(%s, 4326)::geography
            ) as distance_meters,
            CASE
                WHEN tags->>'brand:wikidata' = %s THEN 100
                WHEN LOWER(tags->>'brand') = LOWER(%s) THEN 90
                WHEN LOWER(name) = LOWER(%s) THEN 80
                WHEN tags->>'addr:postcode' = %s AND LOWER(tags->>'addr:city') = LOWER(%s) THEN 70
                WHEN similarity(LOWER(name), LOWER(%s)) > 0.8 THEN 60
                ELSE 0
            END as match_score
        FROM planet_osm_point
        WHERE
            -- Filter by proximity (within 500 meters)
            ST_DWithin(
                way::geography,
                ST_GeomFromText(%s, 4326)::geography,
                500
            )
            AND (
                -- Match by brand:wikidata
                tags->>'brand:wikidata' = %s
                -- Or match by brand name
                OR LOWER(tags->>'brand') = LOWER(%s)
                -- Or match by similar name
                OR similarity(LOWER(name), LOWER(%s)) > 0.6
                -- Or match by exact name
                OR LOWER(name) = LOWER(%s)
                -- Or match by address
                OR (tags->>'addr:postcode' = %s AND LOWER(tags->>'addr:city') = LOWER(%s))
            )
        ORDER BY match_score DESC, distance_meters ASC
        LIMIT 5;
        """

        # Convert DuckDB geometry to WKT format for PostGIS
        # Assuming geom is in WKT format or needs conversion
        wkt_geom = f"POINT({geom})" if isinstance(geom, str) else str(geom)
        
        # Execute query with parameters
        cursor.execute(query, (
            wkt_geom,  # For ST_Distance
            brand_wikidata,  # For brand:wikidata exact match
            brand_name,  # For brand name match
            name,  # For name exact match
            postcode,  # For postcode match
            city,  # For city match
            name,  # For name similarity
            wkt_geom,  # For ST_DWithin
            brand_wikidata,  # For WHERE brand:wikidata
            brand_name,  # For WHERE brand
            name,  # For WHERE similarity
            name,  # For WHERE exact name
            postcode,  # For WHERE postcode
            city  # For WHERE city
        ))
        
        pois = cursor.fetchall()

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
            
            # Iterate on each values to get the OSM POIs
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