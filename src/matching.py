from utils import timer
from psycopg import Cursor
from models import Config


@timer
def execute_query(cursor: Cursor) -> Cursor:
    query = """
        WITH joined_poi AS (
        SELECT
            *,
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
        WHERE pt_cnt <= 1 AND poly_cnt <= 1
    """
    params = [Config.departement_number()]
    if Config.brand():
        query += " AND atp.brand_wikidata = %s"
        params.append(Config.brand())

    if Config.postcode():
        query += " AND atp.postcode = %s"
        params.append(Config.postcode())

    return cursor.execute(query, params)
