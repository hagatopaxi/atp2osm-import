#!/usr/bin/env python3

import argparse
import duckdb
import logging
import sys
import os
import psycopg2

from utils import timer
from models import AtpPoi, OsmPoi, Config
from setup import setup_atp_fr_db, setup_osm_db


logger = logging.getLogger(__name__)
osmdb = psycopg2.connect(
    dbname=os.getenv("OSM_DB_NAME"),
    user=os.getenv("OSM_DB_USER"),
    password=os.getenv("OSM_DB_PASSWORD"),
    host=os.getenv("OSM_DB_HOST"),
    port=os.getenv("OSM_DB_PORT"),
)


def get_osm_poi(atp_poi: AtpPoi, i: int):
    try:
        # Create a cursor for each POI request
        cursor = osmdb.cursor()

        # SQL query to match OSM POIs based on ATP data
        # POI must match on multiple conditions:
        # 1. ATP POI and OSM POI are in the same 500 meters range
        # 2. At least one of the following fields are the same: brand, brand:wikidata, name, email, phone, website
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
                    -- Or match the exact email address
                    OR LOWER(email) = LOWER(%s)
                    -- Or match by exact website (less http[s]://)
                    OR LOWER(REGEXP_REPLACE(website, '^https?://', '', 'i')) = LOWER(REGEXP_REPLACE(%s, '^https?://', '', 'i'))
                    -- Or match by exact phone number (without +33 prefix, replaced by 0, if anywhere)
                    OR REGEXP_REPLACE(REGEXP_REPLACE(phone, '^\+33', '0'), '\s+', '', 'g') = REGEXP_REPLACE(REGEXP_REPLACE(%s, '^\+33', '0'), '\s+', '', 'g')
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
            atp_poi.email,  # For email match
            atp_poi.name,  # For name exact match
            atp_poi.website,  # For website match
            atp_poi.phone,  # For phone match
            atp_poi.postcode,  # For debug
            atp_poi.city,  # For debug
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
                f.write(cursor.mogrify(query, query_params).decode("utf-8"))

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
    pass


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
            logger.info(
                f"Processing {brand_wikidata} {skip} to {min(skip + limit, brand_count)}"
            )

            query_params = [
                brand_wikidata,
            ]
            where_clause = ""
            if Config.postcode() is not None:
                query_params.append(Config.postcode())
                where_clause = " AND postcode = ?"

            atp_pois = duckdb.execute(
                f"""
                SELECT *
                FROM atp_fr
                WHERE brand_wikidata = ? {where_clause}
                LIMIT ? OFFSET ?
            """,
                query_params + [limit, skip],
            ).fetchall()

            # Iterate on each value to get the OSM POIs
            for atp_poi in atp_pois:
                get_osm_poi(AtpPoi(atp_poi), i)
                i += 1


@timer
def main():
    parser = argparse.ArgumentParser(
        prog="atp2osm-import", description="Import ATP FR data into OSM"
    )
    parser.add_argument(
        "-d",
        "--debug",
        action="store_true",
        help="Enable debug mode, that will slow down the process, better to use with filter",
    )
    parser.add_argument(
        "-b", "--brand-wikidata", action="store", help="Brand wikidata filter"
    )
    parser.add_argument("-p", "--postcode", action="store", help="Postcode filter")
    parser.add_argument(
        "--force-atp-setup",
        action="store_true",
        help="Force download and setup the latest ATP data",
    )
    parser.add_argument(
        "--force-osm-setup", action="store_true", help="Force setup the OSM database"
    )

    args = parser.parse_args()
    Config.setup(args)

    logging.basicConfig(
        stream=sys.stdout, level=logging.DEBUG if Config.debug() else logging.INFO
    )

    # 1. Setup ATP_FR table (download and extract)
    setup_atp_fr_db()

    # 2. Setup OSM database (create a view and necessary indexes)
    setup_osm_db(osmdb)

    # 3. For each brands, check if there is an existing POI in OSM, then apply the changes
    compute_changes()


if __name__ == "__main__":
    main()

    # Close the osmdb connection at to end
    if osmdb:
        osmdb.close()
