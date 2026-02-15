from psycopg.rows import dict_row


def get_metadata(osmdb):
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
