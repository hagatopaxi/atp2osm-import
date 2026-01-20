#!/usr/bin/env python3

import argparse
import logging
import sys
import os
import psycopg

from utils import timer, deep_equal
from models import Config
from setup import setup_atp2osm_db
from psycopg.rows import dict_row
from typing import Any


logger = logging.getLogger(__name__)


def apply_tag(tags: dict, key: str, value: Any):
    if key not in tags:
        tags[key] = value


def apply_changes(matched_poi: dict):
    new_tags = dict(matched_poi["tags"])

    apply_tag(new_tags, "opening_hours", matched_poi["atp_opening_hours"])
    apply_tag(new_tags, "addr:country", matched_poi["atp_country"])
    apply_tag(new_tags, "addr:postcode", matched_poi["atp_postcode"])
    apply_tag(new_tags, "addr:city", matched_poi["atp_city"])
    apply_tag(new_tags, "website", matched_poi["atp_website"])

    # Do not duplicate (contact:email and email) or (contact:phone and phone) in tags
    if "contact:email" not in new_tags:
        apply_tag(new_tags, "email", matched_poi["atp_email"])
    if "contact:phone" not in new_tags:
        apply_tag(new_tags, "phone", matched_poi["atp_phone"])

    # If new_tags and original ones are the same we do not try to update the node in OSM
    if not deep_equal(new_tags, matched_poi["tags"]):
        return {
            "osm_id": matched_poi["osm_id"],
            "version": matched_poi["version"],
            "tags": new_tags,
        }


@timer
def compute_changes():
    query = """
        WITH joined_poi AS (
        SELECT
            *,
            atp.opening_hours as atp_opening_hours,
            atp.phone as atp_phone,
            atp.email as atp_email,
            atp.website as atp_website,
            atp.country as atp_country,
            atp.postcode as atp_postcode,
            atp.city as atp_city,
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
            atp.departement_number = %s AND
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

    with osmdb.cursor(row_factory=dict_row) as cursor:
        # Iterate over metropolitan department numbers from 1 to 95
        update_nodes = []
        dep_list = (
            [Config.departement_number()]
            if Config.departement_number() is not None
            else range(1, 96)
        )
        for dep_number in dep_list:
            cursor.execute(query, [dep_number])

            for matched_poi in cursor:
                res = apply_changes(matched_poi)
                if res is not None:
                    update_nodes.append(res)


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

    # 1. Setup the database, that import fresh new data before starting
    setup_atp2osm_db(osmdb)

    # 2. For each brands, check if there is an existing POI in OSM, then apply the changes
    compute_changes()


if __name__ == "__main__":
    with psycopg.connect(
        dbname=os.getenv("OSM_DB_NAME"),
        user=os.getenv("OSM_DB_USER"),
        password=os.getenv("OSM_DB_PASSWORD"),
        host=os.getenv("OSM_DB_HOST"),
        port=os.getenv("OSM_DB_PORT"),
    ) as osmdb:
        main(osmdb)
