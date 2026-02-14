from psycopg.rows import dict_row

from src.matching import execute_query


def get_metadata(osmdb):
    query = """
        SELECT 
            brand_wikidata, 
            (select brand from atp_fr af_2 where af_2.brand_wikidata = af.brand_wikidata limit 1) as brand, 
            count(*) AS total 
        FROM atp_fr af 
        WHERE brand_wikidata is not NULL and brand is not NULL 
        GROUP BY brand_wikidata 
        ORDER BY total desc
        LIMIT 10
    """

    with osmdb.cursor(row_factory=dict_row) as cursor:
        brands = cursor.execute(query).fetchall()

        for brand in brands:
            res = execute_query(cursor, brand=brand["brand_wikidata"]).fetchall()
            brand["total"] = len(res)
            brand["last_import"] = "never"

    brands = sorted(brands, key=lambda brand: brand["last_import"], reverse=True)
    return brands
