import logging

from typing import Any


logger = logging.getLogger(__name__)


def apply_tag(tags: dict, key: str, value: Any) -> None:
    if value is None:
        return
    if key not in tags:
        tags[key] = value


def apply_on_node(atp_osm_match: dict) -> dict:
    new_tags = dict(atp_osm_match["tags"])

    apply_tag(new_tags, "opening_hours", atp_osm_match["atp_opening_hours"])

    # Do not duplicate (contact:email and email) or (contact:phone and phone) or (contact:website and website) in tags
    if "contact:email" not in new_tags:
        apply_tag(new_tags, "email", atp_osm_match["atp_email"])
    if "contact:phone" not in new_tags:
        apply_tag(new_tags, "phone", atp_osm_match["atp_phone"])
    if "contact:website" not in new_tags:
        apply_tag(new_tags, "website", atp_osm_match["atp_website"])

    # If new_tags and original ones are the same returns None to skip the update
    if new_tags == atp_osm_match["tags"]:
        return None

    return {
        "id": atp_osm_match["osm_id"],
        "node_type": atp_osm_match["node_type"],
        "version": atp_osm_match["version"],
        "tag": new_tags,
        "lon": atp_osm_match["lon"],
        "lat": atp_osm_match["lat"],
        "old_tag": atp_osm_match["tags"],
    }
