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
            count(*) FILTER (WHERE osm.node_type = 'node')                 OVER (PARTITION BY atp.id) AS pt_cnt,
            count(*) FILTER (WHERE osm.node_type IN ('way', 'relation'))   OVER (PARTITION BY atp.id) AS poly_cnt
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
        SELECT
            mvb.brand AS brand,
            mvb.brand_wikidata AS brand_wikidata,
            mvb.total AS total,
            ih.last_import,
            ih.last_status
        FROM mv_places_brand mvb
        LEFT JOIN (
            SELECT DISTINCT ON (brand_wikidata)
                brand_wikidata,
                import_date AS last_import,
                status      AS last_status
            FROM import_history
            ORDER BY brand_wikidata, import_date DESC
        ) ih ON ih.brand_wikidata = mvb.brand_wikidata
        WHERE ih.last_import IS NULL
           OR ih.last_status != 'success'
           OR ih.last_import < NOW() - INTERVAL '3 months'
        ORDER BY
            last_import ASC NULLS FIRST,
            total DESC;
    """

    with osmdb.cursor(row_factory=dict_row) as cursor:
        brands = cursor.execute(query).fetchall()

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

    # osm2pgsql's define_area_table stores relation IDs as negative values to
    # distinguish them from way IDs in the shared area_id column. Negate to
    # recover the real OSM ID before passing it to the API or the UI.
    osm_id = atp_osm_match["osm_id"]
    if osm_id < 0:
        osm_id = -osm_id

    return {
        # Values for bulk upload
        "id": osm_id,
        "node_type": atp_osm_match["node_type"],
        "version": atp_osm_match["version"],
        "tag": new_tags,
        "members": atp_osm_match.get("members"),
        "lon": atp_osm_match["lon"],
        "lat": atp_osm_match["lat"],
        # Values only for atp2osm render
        "atp_brand": atp_osm_match["brand"],
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


DEPARTEMENT_NAMES = {
    1: "Ain",
    2: "Aisne",
    3: "Allier",
    4: "Alpes-de-Haute-Provence",
    5: "Hautes-Alpes",
    6: "Alpes-Maritimes",
    7: "Ardèche",
    8: "Ardennes",
    9: "Ariège",
    10: "Aube",
    11: "Aude",
    12: "Aveyron",
    13: "Bouches-du-Rhône",
    14: "Calvados",
    15: "Cantal",
    16: "Charente",
    17: "Charente-Maritime",
    18: "Cher",
    19: "Corrèze",
    21: "Côte-d'Or",
    22: "Côtes-d'Armor",
    23: "Creuse",
    24: "Dordogne",
    25: "Doubs",
    26: "Drôme",
    27: "Eure",
    28: "Eure-et-Loir",
    29: "Finistère",
    30: "Gard",
    31: "Haute-Garonne",
    32: "Gers",
    33: "Gironde",
    34: "Hérault",
    35: "Ille-et-Vilaine",
    36: "Indre",
    37: "Indre-et-Loire",
    38: "Isère",
    39: "Jura",
    40: "Landes",
    41: "Loir-et-Cher",
    42: "Loire",
    43: "Haute-Loire",
    44: "Loire-Atlantique",
    45: "Loiret",
    46: "Lot",
    47: "Lot-et-Garonne",
    48: "Lozère",
    49: "Maine-et-Loire",
    50: "Manche",
    51: "Marne",
    52: "Haute-Marne",
    53: "Mayenne",
    54: "Meurthe-et-Moselle",
    55: "Meuse",
    56: "Morbihan",
    57: "Moselle",
    58: "Nièvre",
    59: "Nord",
    60: "Oise",
    61: "Orne",
    62: "Pas-de-Calais",
    63: "Puy-de-Dôme",
    64: "Pyrénées-Atlantiques",
    65: "Hautes-Pyrénées",
    66: "Pyrénées-Orientales",
    67: "Bas-Rhin",
    68: "Haut-Rhin",
    69: "Rhône",
    70: "Haute-Saône",
    71: "Saône-et-Loire",
    72: "Sarthe",
    73: "Savoie",
    74: "Haute-Savoie",
    75: "Paris",
    76: "Seine-Maritime",
    77: "Seine-et-Marne",
    78: "Yvelines",
    79: "Deux-Sèvres",
    80: "Somme",
    81: "Tarn",
    82: "Tarn-et-Garonne",
    83: "Var",
    84: "Vaucluse",
    85: "Vendée",
    86: "Vienne",
    87: "Haute-Vienne",
    88: "Vosges",
    89: "Yonne",
    90: "Territoire de Belfort",
    91: "Essonne",
    92: "Hauts-de-Seine",
    93: "Seine-Saint-Denis",
    94: "Val-de-Marne",
    95: "Val-d'Oise",
}


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

    by_department = {
        dpt: {"name": DEPARTEMENT_NAMES.get(dpt, "?"), "count": count}
        for dpt, count in sorted(dept_changes.items())
    }

    return {
        "by_tag": tag_updates,
        "size": len(changes),
        "total_tag_updates": total_tag_updates,
        "by_department": by_department,
    }
