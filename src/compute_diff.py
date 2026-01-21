from utils import timer, deep_equal
from typing import Any
from psycopg import Cursor


def apply_tag(tags: dict, key: str, value: Any):
    if key not in tags:
        tags[key] = value


def apply_on_node(atp_osm_match: dict):
    new_tags = dict(atp_osm_match["tags"])

    apply_tag(new_tags, "opening_hours", atp_osm_match["atp_opening_hours"])
    apply_tag(new_tags, "addr:country", atp_osm_match["atp_country"])
    apply_tag(new_tags, "addr:postcode", atp_osm_match["atp_postcode"])
    apply_tag(new_tags, "addr:city", atp_osm_match["atp_city"])
    apply_tag(new_tags, "website", atp_osm_match["atp_website"])

    # Do not duplicate (contact:email and email) or (contact:phone and phone) in tags
    if "contact:email" not in new_tags:
        apply_tag(new_tags, "email", atp_osm_match["atp_email"])
    if "contact:phone" not in new_tags:
        apply_tag(new_tags, "phone", atp_osm_match["atp_phone"])

    # If new_tags and original ones are the same returns None to skip the update
    if deep_equal(new_tags, atp_osm_match["tags"]):
        return None

    return {
        "osm_id": atp_osm_match["osm_id"],
        "version": atp_osm_match["version"],
        "tags": new_tags,
    }


@timer
def apply_changes(cursor: Cursor):
    update_nodes = []
    for atp_osm_match in cursor:
        res = apply_on_node(atp_osm_match)
        if res is not None:
            update_nodes.append(res)

    return update_nodes
