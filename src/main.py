#!/usr/bin/env python3

import argparse
import logging
import sys
import os
import psycopg2

from utils import timer
from models import Config
from setup import setup_atp2osm_db


logger = logging.getLogger(__name__)
osmdb = psycopg2.connect(
    dbname=os.getenv("OSM_DB_NAME"),
    user=os.getenv("OSM_DB_USER"),
    password=os.getenv("OSM_DB_PASSWORD"),
    host=os.getenv("OSM_DB_HOST"),
    port=os.getenv("OSM_DB_PORT"),
)


def apply_changes(matched_poi):
    pass


@timer
def compute_changes():
    query = """
        WITH joined_poi AS (
        SELECT
            *, 
            count(*) FILTER (WHERE osm.node_type = 'point') OVER (PARTITION BY atp.id) AS pt_cnt, 
            count(*) FILTER (WHERE osm.node_type = 'polygon') OVER (PARTITION BY atp.id) AS poly_cnt
        FROM
            mv_places osm
        INNER JOIN atp_fr atp ON
            ST_DWithin(
                geom_9794,
                ST_Transform(ST_GeomFromGeoJSON(atp.geom), 9794),
                500
            )
        WHERE
            atp.departement_number = ? AND
            ( 
                osm.brand_wikidata = atp.brand_wikidata
                OR LOWER(osm.brand) = LOWER(atp.brand)
                OR LOWER(osm.name) = LOWER(atp."name")
                OR LOWER(osm.email) = LOWER(atp.email)
                OR LOWER(REGEXP_REPLACE(osm.website, '^https?://', '', 'i')) = LOWER(REGEXP_REPLACE(atp.website, '^https?://', '', 'i'))
                OR REGEXP_REPLACE(REGEXP_REPLACE(osm.phone, '^\+33', '0'), '\s+', '', 'g') = REGEXP_REPLACE(REGEXP_REPLACE(atp.phone, '^\+33', '0'), '\s+', '', 'g')
            )
        )
        SELECT *
        FROM joined_poi
        WHERE pt_cnt <= 1 AND poly_cnt <= 1;
    """
    cursor = osmdb.cursor()

    # Iterate over metropolitan department numbers from 1 to 95
    for dep_number in range(1, 96):
        matched_pois = cursor.execute(query, [dep_number]).fetchall()

        for matched_poi in matched_pois:
            apply_changes(matched_poi)


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
    parser.add_argument(
        "--force-atp-dl",
        action="store_true",
        help="Force to download the last ATP dump",
    )

    args = parser.parse_args()
    Config.setup(args)

    logging.basicConfig(
        stream=sys.stdout, level=logging.DEBUG if Config.debug() else logging.INFO
    )

    # 1. Setup the database, that import fresh new data before starting
    setup_atp2osm_db(osmdb)

    # 2. For each brands, check if there is an existing POI in OSM, then apply the changes
    compute_changes()


if __name__ == "__main__":
    main()

    # Close the osmdb connection at to end
    if osmdb:
        osmdb.close()
