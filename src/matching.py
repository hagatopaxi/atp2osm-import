from psycopg import Cursor
from psycopg.rows import dict_row
from typing import Any


def get_filtered(
    cursor: Cursor,
    brand: str = None,
    postcode: str = None,
    departement_number: str = None,
) -> Cursor:
    query = """
        WITH joined_poi AS (
        SELECT
            *,
            osm.tags as old_tags,
            ST_X(ST_Centroid(ST_Transform(osm.geom, 4326))) AS lon,
            ST_Y(ST_Centroid(ST_Transform(osm.geom, 4326))) AS lat,
            atp.opening_hours as atp_opening_hours,
            atp.phone as atp_phone,
            atp.email as atp_email,
            atp.website as atp_website,
            atp.country as atp_country,
            atp.city as atp_city,
            atp.source_uri as atp_source_uri,
            count(*) FILTER (WHERE osm.node_type = 'node') OVER (PARTITION BY atp.id) AS pt_cnt, 
            count(*) FILTER (WHERE osm.node_type = 'relation') OVER (PARTITION BY atp.id) AS poly_cnt
        FROM
            mv_places osm
        INNER JOIN atp_fr atp ON
            ST_DWithin(
                geom_9794,
                ST_Transform(ST_GeomFromGeoJSON(atp.geom), 9794),
                500
            )
        WHERE
            {where_options} AND
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
        WHERE pt_cnt <= 1 AND poly_cnt <= 1
    """
    options = []
    params = []
    if brand:
        options.append("atp.brand_wikidata = %s")
        params.append(brand)
    if postcode:
        options.append("atp.postcode = %s")
        params.append(postcode)
    if departement_number:
        options.append("atp.departement_number = %s")
        params.append(departement_number)

    where_options = " AND ".join(options)

    return cursor.execute(query.format(where_options=where_options), params)


def get_all(osmdb):
    query = """
        WITH matched_poi AS (
            WITH joined_poi AS (
                SELECT
                    atp.brand as atp_brand,
                    atp.brand_wikidata as atp_brand_wikidata,
                    ST_X(ST_Centroid(ST_Transform(osm.geom, 4326))) AS lon,
                    ST_Y(ST_Centroid(ST_Transform(osm.geom, 4326))) AS lat,
                    atp.opening_hours as atp_opening_hours,
                    atp.phone as atp_phone,
                    atp.email as atp_email,
                    atp.website as atp_website,
                    atp.country as atp_country,
                    atp.postcode as atp_postcode,
                    atp.city as atp_city,
                    count(*) FILTER (WHERE osm.node_type = 'node') OVER (PARTITION BY atp.id) AS pt_cnt,
                    count(*) FILTER (WHERE osm.node_type = 'relation') OVER (PARTITION BY atp.id) AS poly_cnt
                FROM
                    mv_places osm
                INNER JOIN atp_fr atp ON
                    ST_DWithin(
                        geom_9794,
                        ST_Transform(ST_GeomFromGeoJSON(atp.geom), 9794),
                        500
                    )
                WHERE
                    atp.source_type != 'api' AND
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
            WHERE pt_cnt <= 1 AND poly_cnt <= 1
        )
        -- New aggregation query
        SELECT
            atp_brand AS brand,
            atp_brand_wikidata AS brand_wikidata,
            COUNT(*) AS total
        FROM
            matched_poi
        GROUP BY
            atp_brand, atp_brand_wikidata
        ORDER BY
            total DESC;
    """

    with osmdb.cursor(row_factory=dict_row) as cursor:
        brands = cursor.execute(query).fetchall()

        for brand in brands:
            brand["last_import"] = "never"

    brands = sorted(brands, key=lambda brand: brand["last_import"], reverse=True)
    return brands


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
        # Values for bulk upload
        "id": atp_osm_match["osm_id"],
        "node_type": atp_osm_match["node_type"],
        "version": atp_osm_match["version"],
        "tag": new_tags,
        "lon": atp_osm_match["lon"],
        "lat": atp_osm_match["lat"],
        # Values only for atp2osm render
        "source_uri": atp_osm_match["source_uri"],
        "postcode": atp_osm_match["postcode"],
        "old_tag": atp_osm_match["tags"],
        "departement_number": atp_osm_match["departement_number"],
    }


def add_result(nodes_by_brand, brand_wikidata, res):
    if brand_wikidata in nodes_by_brand:
        nodes_by_brand[brand_wikidata].append(res)
    else:
        nodes_by_brand[brand_wikidata] = [res]


def get_changes(cursor: Cursor):
    changes = []

    for atp_osm_match in cursor:
        res = apply_on_node(atp_osm_match)
        if res is None:
            continue
        changes.append(res)

    return changes


def get_stats(changes: list) -> dict:
    tag_updates = {}
    total_tag_updates = 0
    dept_changes = {}

    for change in changes:
        # Count tag updates
        tag = change.get("tag", {})
        old_tag = change.get("old_tag", {})
        updated_tags = set(tag.keys()).union(set(old_tag.keys()))
        for t in updated_tags:
            if tag.get(t) != old_tag.get(t):
                tag_updates[t] = tag_updates.get(t, 0) + 1
                total_tag_updates += 1

        # Count changes by department
        dpt = change.get("departement_number", 0)
        if dpt != 0:
            dept_changes[dpt] = dept_changes.get(dpt, 0) + 1

    return {
        "by_tag": tag_updates,
        "size": len(changes),
        "total_tag_updates": total_tag_updates,
        "by_department": dept_changes,
    }
