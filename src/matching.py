import random
import re

from psycopg import Cursor
from psycopg.rows import dict_row
from typing import Any, NamedTuple


# Requête unique de correspondance ATP <-> OSM, partagée par /validate
# (get_filtered) et par la vue matérialisée mv_places_brand qui alimente le
# compteur de la liste des marques. Les deux DOIVENT rester sur la même SQL :
# c'est la divergence entre deux copies qui a fait afficher 50 POIs dans la
# liste et 60 dans /validate.
#
# Le dédoublonnage inclut atp_brand_wikidata : un même objet OSM peut
# correspondre à deux marques différentes, et il doit alors être compté pour
# chacune (comme le fait /validate, qui filtre sur une seule marque).
MATCHED_POI_SQL = """
    WITH joined_poi AS (
    SELECT
        *,
        osm.tags as old_tags,
        ST_X(ST_Centroid(osm.geom)) AS lon,
        ST_Y(ST_Centroid(osm.geom)) AS lat,
        atp.opening_hours as atp_opening_hours,
        atp.phone as atp_phone,
        atp.email as atp_email,
        atp.website as atp_website,
        atp.country as atp_country,
        atp.city as atp_city,
        atp.source_uri as atp_source_uri,
        atp.brand as atp_brand,
        atp.brand_wikidata as atp_brand_wikidata,
        (
            (atp.opening_hours IS NOT NULL AND osm.opening_hours IS NULL)
            OR (atp.email   IS NOT NULL AND osm.email   IS NULL)
            OR (atp.phone   IS NOT NULL AND osm.phone   IS NULL)
            OR (atp.website IS NOT NULL AND osm.website IS NULL)
        ) AS is_importable,
        ST_Distance(osm.geom::geography, ST_GeomFromGeoJSON(atp.geom)::geography) AS atp_distance,
        count(*) FILTER (WHERE osm.node_type = 'node')                 OVER (PARTITION BY atp.id) AS pt_cnt,
        count(*) FILTER (WHERE osm.node_type IN ('way', 'relation'))   OVER (PARTITION BY atp.id) AS poly_cnt
    FROM
        mv_places osm
    INNER JOIN atp_fr atp ON
        ST_DWithin(
            osm.geom::geography,
            ST_GeomFromGeoJSON(atp.geom)::geography,
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
            OR normalize_phone(osm.phone) = normalize_phone(atp.phone)
        )
    )
    SELECT DISTINCT ON (osm_id, node_type, atp_brand_wikidata) *
    FROM joined_poi
    WHERE pt_cnt <= 1 AND poly_cnt <= 1
    ORDER BY osm_id, node_type, atp_brand_wikidata, atp_distance
"""


def get_filtered(
    cursor: Cursor,
    brand: str = None,
    postcode: str = None,
    departement_number: str = None,
) -> Cursor:
    query = MATCHED_POI_SQL
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

    where_options = " AND ".join(options) or "TRUE"

    return cursor.execute(query.format(where_options=where_options), params)


# Cooldowns: how long an import keeps hiding what it just touched, until the
# daily refresh drops the integrated POIs from the matches.
SUCCESS_COOLDOWN = "3 months"
ERROR_COOLDOWN = "4 weeks"

# Cooldowns are code constants, never values coming from a request: splicing
# them into the SQL below cannot inject anything. The format is checked at import
# time so that it stays that way.
assert all(
    re.fullmatch(r"\d+ (days|weeks|months)", cooldown)
    for cooldown in (SUCCESS_COOLDOWN, ERROR_COOLDOWN)
)


def _within(cooldown: str) -> str:
    """SQL condition: the import is still within its cooldown."""
    return f"ih.import_date > NOW() - INTERVAL '{cooldown}'"


# Départements still under cooldown, one row per (brand, département). Shared
# between get_all() (the list count) and get_blocked_departements() (batch
# composition): both must block exactly the same ones.
BLOCKED_DEPARTEMENTS_SQL = f"""
    SELECT ih.brand_wikidata, dpt.departement_number
    FROM import_departements dpt
    JOIN import_history ih ON ih.id = dpt.import_id
    WHERE (dpt.status IN ('error_osm_api','error_unknown') AND {_within(ERROR_COOLDOWN)})
       OR (dpt.status = 'success'                          AND {_within(SUCCESS_COOLDOWN)})
"""

# Imports with no changeset at all: a cancellation, a brand with nothing left to
# integrate, or a pre-migration row the backfill could not detail. They point at
# no département in particular, so they hide the whole brand for the cooldown.
#
# `partial` is absent: it implies some départements succeeded and others failed,
# hence child rows. The backfill (migration 016) detailed them all, and no import
# produces a childless one any more.
BLOCKED_BRANDS_SQL = f"""
    SELECT ih.*
    FROM import_history ih
    WHERE NOT EXISTS (SELECT 1 FROM import_departements dpt WHERE dpt.import_id = ih.id)
      AND (
        (ih.status IN ('cancelled', 'error') AND {_within(ERROR_COOLDOWN)})
        OR (ih.status = 'success'            AND {_within(SUCCESS_COOLDOWN)})
      )
"""


def get_all(osmdb):
    # `total` is the number of POIs *left to integrate*: départements under
    # cooldown are excluded, and brands blocked as a whole drop out entirely.
    query = f"""
        WITH blocked AS (
            SELECT brand_wikidata, ARRAY_AGG(DISTINCT departement_number) AS dpts
            FROM ({BLOCKED_DEPARTEMENTS_SQL}) b
            GROUP BY brand_wikidata
        )
        SELECT
            MAX(mvb.brand) AS brand,
            mvb.brand_wikidata AS brand_wikidata,
            SUM(mvb.total) AS total,
            ih.last_import,
            ih.last_status
        FROM mv_places_brand mvb
        LEFT JOIN blocked ON blocked.brand_wikidata = mvb.brand_wikidata
        LEFT JOIN (
            SELECT DISTINCT ON (brand_wikidata)
                brand_wikidata,
                import_date AS last_import,
                status      AS last_status
            FROM import_history
            ORDER BY brand_wikidata, import_date DESC
        ) ih ON ih.brand_wikidata = mvb.brand_wikidata
        WHERE (mvb.brand IS NOT NULL AND mvb.brand_wikidata IS NOT NULL)
          AND NOT (COALESCE(mvb.departement_number, '') = ANY(COALESCE(blocked.dpts, '{{}}')))
          AND NOT EXISTS (
              SELECT 1 FROM ({BLOCKED_BRANDS_SQL}) blocked_brands
              WHERE blocked_brands.brand_wikidata = mvb.brand_wikidata
          )
        GROUP BY mvb.brand_wikidata, ih.last_import, ih.last_status
        ORDER BY
            SUM(mvb.total) DESC,
            ih.last_import ASC NULLS FIRST;
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
        "atp_id": atp_osm_match["id"],
        "spider_id": atp_osm_match.get("spider_id"),
        "source_uri": atp_osm_match["source_uri"],
        "source_type": atp_osm_match["source_type"],
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
    # seen = set()

    for atp_osm_match in cursor:
        # key = (atp_osm_match["osm_id"], atp_osm_match["node_type"])
        # if key in seen:
        #     continue
        # seen.add(key)

        res = apply_on_node(atp_osm_match)
        if res is None:
            continue
        changes.append(res)

    return changes


DEPARTEMENT_NAMES = {
    "01": "Ain",
    "02": "Aisne",
    "03": "Allier",
    "04": "Alpes-de-Haute-Provence",
    "05": "Hautes-Alpes",
    "06": "Alpes-Maritimes",
    "07": "Ardèche",
    "08": "Ardennes",
    "09": "Ariège",
    "10": "Aube",
    "11": "Aude",
    "12": "Aveyron",
    "13": "Bouches-du-Rhône",
    "14": "Calvados",
    "15": "Cantal",
    "16": "Charente",
    "17": "Charente-Maritime",
    "18": "Cher",
    "19": "Corrèze",
    # Corse — les codes postaux ATP sont 20xxx (pas 2Axxx/2Bxxx), donc
    # departement_number vaut "20". Les codes "2A"/"2B" seraient utiles si
    # on affinait un jour par plage de CP (20000-20199 → 2A, 20200+ → 2B).
    "20": "Corse",
    "2A": "Corse-du-Sud",
    "2B": "Haute-Corse",
    "21": "Côte-d'Or",
    "22": "Côtes-d'Armor",
    "23": "Creuse",
    "24": "Dordogne",
    "25": "Doubs",
    "26": "Drôme",
    "27": "Eure",
    "28": "Eure-et-Loir",
    "29": "Finistère",
    "30": "Gard",
    "31": "Haute-Garonne",
    "32": "Gers",
    "33": "Gironde",
    "34": "Hérault",
    "35": "Ille-et-Vilaine",
    "36": "Indre",
    "37": "Indre-et-Loire",
    "38": "Isère",
    "39": "Jura",
    "40": "Landes",
    "41": "Loir-et-Cher",
    "42": "Loire",
    "43": "Haute-Loire",
    "44": "Loire-Atlantique",
    "45": "Loiret",
    "46": "Lot",
    "47": "Lot-et-Garonne",
    "48": "Lozère",
    "49": "Maine-et-Loire",
    "50": "Manche",
    "51": "Marne",
    "52": "Haute-Marne",
    "53": "Mayenne",
    "54": "Meurthe-et-Moselle",
    "55": "Meuse",
    "56": "Morbihan",
    "57": "Moselle",
    "58": "Nièvre",
    "59": "Nord",
    "60": "Oise",
    "61": "Orne",
    "62": "Pas-de-Calais",
    "63": "Puy-de-Dôme",
    "64": "Pyrénées-Atlantiques",
    "65": "Hautes-Pyrénées",
    "66": "Pyrénées-Orientales",
    "67": "Bas-Rhin",
    "68": "Haut-Rhin",
    "69": "Rhône",
    "70": "Haute-Saône",
    "71": "Saône-et-Loire",
    "72": "Sarthe",
    "73": "Savoie",
    "74": "Haute-Savoie",
    "75": "Paris",
    "76": "Seine-Maritime",
    "77": "Seine-et-Marne",
    "78": "Yvelines",
    "79": "Deux-Sèvres",
    "80": "Somme",
    "81": "Tarn",
    "82": "Tarn-et-Garonne",
    "83": "Var",
    "84": "Vaucluse",
    "85": "Vendée",
    "86": "Vienne",
    "87": "Haute-Vienne",
    "88": "Vosges",
    "89": "Yonne",
    "90": "Territoire de Belfort",
    "91": "Essonne",
    "92": "Hauts-de-Seine",
    "93": "Seine-Saint-Denis",
    "94": "Val-de-Marne",
    "95": "Val-d'Oise",
    # Départements et régions d'outre-mer (DROM)
    "971": "Guadeloupe",
    "972": "Martinique",
    "973": "Guyane",
    "974": "La Réunion",
    "976": "Mayotte",
    # Collectivités d'outre-mer (COM)
    "975": "Saint-Pierre-et-Miquelon",
    "977": "Saint-Barthélemy",
    "978": "Saint-Martin",
    "984": "Terres australes et antarctiques françaises",
    "986": "Wallis-et-Futuna",
    "987": "Polynésie française",
    "988": "Nouvelle-Calédonie",
}


def pack_departements(counts: dict[str, int], max_size: int) -> list[list[str]]:
    """Group départements into batches of at most *max_size* POIs.

    Greedy first-fit-decreasing: start a batch with the biggest département left,
    then keep adding the biggest one that still fits, close the batch when none
    does. A département bigger than *max_size* gets a batch of its own; the
    caller truncates it to *max_size* POIs and the remainder waits for the
    cooldown to expire.

    Returns batches as sorted lists of département codes, biggest batch first.
    """
    # clamping makes an oversized département fill a batch on its own
    remaining = sorted(
        ((dpt, min(n, max_size)) for dpt, n in counts.items()),
        key=lambda kv: (-kv[1], kv[0]),
    )
    batches = []

    while remaining:
        batch = [remaining.pop(0)]
        room = max_size - batch[0][1]
        # remaining stays sorted by size desc, so the first that fits is the biggest
        while (
            i := next((i for i, (_, n) in enumerate(remaining) if n <= room), None)
        ) is not None:
            dpt, n = remaining.pop(i)
            batch.append((dpt, n))
            room -= n
        batches.append(sorted(dpt for dpt, _ in batch))

    return batches


# Biggest batch, in POIs. A batch = a set of whole départements, one changeset
# per département.
#
# Capped at 100 during the beta: this is what a single human action uploads at
# once. The spec plans for 200, to be raised once the platform has matured.
BATCH_MAX_SIZE = 100

# Hard ceiling: past that, a batch is refused rather than uploaded. Composition
# targets BATCH_MAX_SIZE, so the gap between the two is pure slack — it lets the
# beta cap be lowered without the safety nets firing on a legitimate batch.
MAX_UPLOAD_SIZE = 200

# Floor of POIs reviewed per batch — the sample may exceed it to cover every
# changed tag. A batch smaller than that is reviewed in full.
BATCH_SAMPLE_SIZE = 3


def changed_tags(change: dict) -> set[str]:
    """Keys whose value differs between the existing POI and the proposal."""
    tag = change.get("tag", {})
    old_tag = change.get("old_tag", {})
    return {k for k in tag.keys() | old_tag.keys() if tag.get(k) != old_tag.get(k)}


def sample_for_review(changes: list[dict], min_size: int = BATCH_SAMPLE_SIZE) -> list[dict]:
    """Sample reviewed before integration: at least one POI per changed tag.

    Its size therefore follows the number of tags involved, topped up at
    random up to *min_size*.
    """
    by_tag: dict[str, list[int]] = {}
    for i, change in enumerate(changes):
        for tag in changed_tags(change):
            by_tag.setdefault(tag, []).append(i)

    picked: set[int] = set()
    # ponytail: naive greedy, not a minimal cover — a few POIs too many at
    # worst, and the tag count stays single-digit.
    for candidates in by_tag.values():
        if not picked.intersection(candidates):
            picked.add(random.choice(candidates))

    rest = [i for i in range(len(changes)) if i not in picked]
    picked.update(random.sample(rest, max(0, min(min_size - len(picked), len(rest)))))
    return [changes[i] for i in sorted(picked)]


def count_by_departement(changes: list[dict]) -> dict[str, int]:
    """Match count per département."""
    counts = {}
    for change in changes:
        dpt = change["departement_number"]
        counts[dpt] = counts.get(dpt, 0) + 1
    return counts


def get_blocked_departements(cursor: Cursor, brand_wikidata: str) -> set[str]:
    """Départements of the brand still under cooldown.

    The status that counts is the changeset's, not the import's: a changeset
    either went through or did not, there is no partial status at that level.
    """
    rows = cursor.execute(
        f"""SELECT DISTINCT departement_number AS dpt
            FROM ({BLOCKED_DEPARTEMENTS_SQL}) b
            WHERE brand_wikidata = %s""",
        (brand_wikidata,),
    ).fetchall()
    return {row["dpt"] for row in rows}  # dict_row cursor, as everywhere here


def compose_batch(
    counts: dict[str, int], blocked: set[str], max_size: int = BATCH_MAX_SIZE
) -> list[str]:
    """Départements of the next batch to integrate.

    Never persisted: recomputed on every visit from the current state. Returns an
    empty list when every département is blocked.
    """
    available = {dpt: n for dpt, n in counts.items() if dpt not in blocked}
    batches = pack_departements(available, max_size)
    return batches[0] if batches else []


class Batch(NamedTuple):
    changes: list[dict]  # what will be integrated
    scope: list[dict]    # its départements, for display: number, name, count


def select_batch(
    changes: list[dict], blocked: set[str], max_size: int = BATCH_MAX_SIZE
) -> Batch:
    """Narrow matches down to the next batch.

    A batch is made of whole départements: one that does not fit in the room left
    moves to the next batch, it is never cut. A batch below *max_size* is
    therefore normal — it happens as soon as no remaining département fills the
    gap.

    The only possible truncation is a département bigger than *max_size* on its
    own: it then forms a batch by itself (pack_departements leaves it no room),
    and its extra POIs wait for the cooldown to expire.

    """
    batch = set(compose_batch(count_by_departement(changes), blocked, max_size))
    changes = [c for c in changes if c["departement_number"] in batch]
    # A no-op on a multi-département batch, which fits in max_size by
    # construction. Truncating before the sample is drawn keeps the review on
    # POIs that will actually be integrated.
    changes = changes[:max_size]

    scope = [
        {"number": dpt, "name": DEPARTEMENT_NAMES.get(dpt, dpt), "count": count}
        for dpt, count in sorted(
            count_by_departement(changes).items(), key=lambda kv: (-kv[1], kv[0])
        )
    ]
    return Batch(changes, scope)


def get_stats(changes: list) -> dict:
    tag_updates = {}
    total_tag_updates = 0
    dept_changes = {}

    for change in changes:
        # Count tag updates
        for t in changed_tags(change):
            tag_updates[t] = tag_updates.get(t, 0) + 1
            total_tag_updates += 1

        # Count changes by department
        dpt = change.get("departement_number")
        if dpt is not None:
            dept_changes[dpt] = dept_changes.get(dpt, 0) + 1

    by_department = {
        dpt: {"name": DEPARTEMENT_NAMES.get(dpt, dpt), "count": count}
        for dpt, count in sorted(dept_changes.items())
    }

    return {
        "by_tag": tag_updates,
        "size": len(changes),
        "total_tag_updates": total_tag_updates,
        "by_department": by_department,
    }
