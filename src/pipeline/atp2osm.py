import logging

from src.pipeline._db import connect

logger = logging.getLogger(__name__)


def create_mv_places_brand():
    conn = connect()
    try:
        with conn.cursor() as cur:
            cur.execute("DROP MATERIALIZED VIEW IF EXISTS mv_places_brand;")
            logger.info("Creating mv_places_brand...")
            cur.execute("""
                CREATE MATERIALIZED VIEW mv_places_brand AS
                WITH joined_poi AS (
                    SELECT
                        osm.osm_id,
                        osm.node_type,
                        atp.brand            AS atp_brand,
                        atp.brand_wikidata   AS atp_brand_wikidata,
                        (
                            (atp.opening_hours IS NOT NULL AND osm.opening_hours IS NULL)
                            OR (atp.email    IS NOT NULL AND osm.email    IS NULL)
                            OR (atp.phone    IS NOT NULL AND osm.phone    IS NULL)
                            OR (atp.website  IS NOT NULL AND osm.website  IS NULL)
                        ) AS is_importable,
                        ST_Distance(osm.geom::geography, ST_GeomFromGeoJSON(atp.geom)::geography) AS atp_distance,
                        count(*) FILTER (WHERE osm.node_type = 'node')               OVER (PARTITION BY atp.id) AS pt_cnt,
                        count(*) FILTER (WHERE osm.node_type IN ('way', 'relation')) OVER (PARTITION BY atp.id) AS poly_cnt
                    FROM mv_places osm
                    INNER JOIN atp_fr atp ON
                        ST_DWithin(osm.geom::geography, ST_GeomFromGeoJSON(atp.geom)::geography, 500)
                    WHERE
                        osm.brand_wikidata = atp.brand_wikidata
                        OR LOWER(osm.brand) = LOWER(atp.brand)
                        OR LOWER(osm.name)  = LOWER(atp."name")
                        OR LOWER(osm.email) = LOWER(atp.email)
                        OR LOWER(REGEXP_REPLACE(osm.website, '^https?://', '', 'i')) = LOWER(REGEXP_REPLACE(atp.website, '^https?://', '', 'i'))
                        OR normalize_phone(osm.phone) = normalize_phone(atp.phone)
                ),
                deduped AS (
                    SELECT DISTINCT ON (osm_id, node_type) *
                    FROM joined_poi
                    WHERE pt_cnt <= 1 AND poly_cnt <= 1 AND is_importable
                    ORDER BY osm_id, node_type, atp_distance
                )
                SELECT
                    STRING_AGG(DISTINCT atp_brand, ' / ' ORDER BY atp_brand) AS brand,
                    atp_brand_wikidata AS brand_wikidata,
                    COUNT(*)           AS total
                FROM deduped
                GROUP BY atp_brand_wikidata
            """)
        conn.commit()
        logger.info("mv_places_brand created")
    finally:
        conn.close()
