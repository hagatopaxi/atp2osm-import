from typing import Any


def apply_tag(tags: dict, key: str, value: Any):
    if value is None:
        return
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
    if new_tags == atp_osm_match["tags"]:
        return None

    return {
        "id": atp_osm_match["osm_id"],
        "node_type": atp_osm_match["node_type"],
        "version": atp_osm_match["version"],
        "tag": new_tags,
        "lon": atp_osm_match["lon"],
        "lat": atp_osm_match["lat"],
    }
