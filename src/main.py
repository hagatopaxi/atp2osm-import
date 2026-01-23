#!/usr/bin/env python3

import argparse
import logging
import sys
import os
import psycopg

from utils import timer, clean_debug_folder
from models import Config
from setup import setup_atp2osm_db
from matching import execute_query
from compute_diff import apply_on_node
from psycopg.rows import dict_row
from upload import BulkUpload
from psycopg import Cursor


logger = logging.getLogger(__name__)


@timer
def get_changes(cursor: Cursor):
    nodes_by_brand = {"no_brand": []}

    for atp_osm_match in cursor:
        res = apply_on_node(atp_osm_match)
        if res is not None:
            brand_wikidata = (
                res["tag"]["brand:wikidata"] if "brand:wikidata" in res["tag"] else None
            )
            if brand_wikidata is None:
                nodes_by_brand["no_brand"].append(res)
            else:
                if brand_wikidata in nodes_by_brand:
                    nodes_by_brand[brand_wikidata].append(res)
                else:
                    nodes_by_brand[brand_wikidata] = [res]

    return nodes_by_brand


@timer
def main(osmdb):
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
    parser.add_argument(
        "-n",
        "--departement-number",
        action="store",
        help="Specify a departement number from 1 to 95",
    )

    args = parser.parse_args()
    Config.setup(args)

    logging.basicConfig(
        stream=sys.stdout, level=logging.DEBUG if Config.debug() else logging.INFO
    )

    if Config.departement_number():
        clean_debug_folder()

    # 1. Setup the database, that import fresh new data before starting
    setup_atp2osm_db(osmdb)

    with osmdb.cursor(row_factory=dict_row) as cursor:
        # 2. For each brands, check if there is an existing POI in OSM, then apply the changes
        execute_query(cursor)

        # 3. Iterate on the cursor to apply changes
        changes = get_changes(cursor)

    # 4. Upload changes into OSM
    BulkUpload(changes)


if __name__ == "__main__":
    with psycopg.connect(
        dbname=os.getenv("OSM_DB_NAME"),
        user=os.getenv("OSM_DB_USER"),
        password=os.getenv("OSM_DB_PASSWORD"),
        host=os.getenv("OSM_DB_HOST"),
        port=os.getenv("OSM_DB_PORT"),
    ) as osmdb:
        main(osmdb)
