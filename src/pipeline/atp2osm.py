import logging

from src.matching import MATCHED_POI_SQL
from src.pipeline._db import connect

logger = logging.getLogger(__name__)


def create_mv_places_brand():
    conn = connect()
    try:
        with conn.cursor() as cur:
            cur.execute("DROP MATERIALIZED VIEW IF EXISTS mv_places_brand;")
            logger.info("Creating mv_places_brand...")
            # is_importable est filtré APRÈS le dédoublonnage, comme
            # apply_on_node() côté /validate, sinon les deux comptages divergent.
            # One row per (brand, département): get_all() sums the unblocked
            # ones to announce what is still left to integrate.
            cur.execute(f"""
                CREATE MATERIALIZED VIEW mv_places_brand AS
                SELECT
                    STRING_AGG(DISTINCT atp_brand, ' / ' ORDER BY atp_brand) AS brand,
                    atp_brand_wikidata AS brand_wikidata,
                    departement_number,
                    COUNT(*)           AS total
                FROM ({MATCHED_POI_SQL.format(where_options="TRUE")}) matched
                WHERE is_importable
                GROUP BY atp_brand_wikidata, departement_number
            """)
        conn.commit()
        logger.info("mv_places_brand created")
    finally:
        conn.close()
