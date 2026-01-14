import logging
import requests
import sys
import os
import duckdb


from models import Config
from utils import delete_file_if_exists, timer


logger = logging.getLogger(__name__)


def download_latest_atp_data():
    if Config.force_atp_setup():
        logger.info("Forcing download of latest ATP data")
        delete_file_if_exists("./data/atp/latest.parquet")
        delete_file_if_exists("./data/atp/atp_fr.parquet")

    download_path = "./data/atp/latest.parquet"

    # If the download_path file is already there, skip the download
    if os.path.exists(download_path):
        logger.info(f"{download_path} already exists, skipping download")
        return

    url = "https://data.alltheplaces.xyz/runs/latest.json"
    response = requests.get(url)
    if response.status_code != 200:
        logger.error(f"Failed to download {url}")
        sys.exit(1)

    data = response.json()
    parquet_url = data.get("parquet_url")
    # If the download_path directory doesn't exist, create it
    os.makedirs(os.path.dirname(download_path), exist_ok=True)

    if not parquet_url:
        logger.error("'parquet_url' key not found in JSON response")
        sys.exit(1)

    logger.info("Downloading latest ATP data")
    parquet_response = requests.get(parquet_url)
    if parquet_response.status_code != 200:
        logger.error(f"Failed to download {parquet_url}")
        sys.exit(1)

    with open(download_path, "wb") as file:
        file.write(parquet_response.content)

    logger.info(f"Downloaded {download_path}")


@timer
def setup_atp_fr_db():
    download_latest_atp_data()

    duckdb.sql("INSTALL spatial; LOAD spatial;")

    if os.path.exists("./data/atp/atp_fr.parquet"):
        logger.info("Loading existing atp_fr.parquet")
        duckdb.read_parquet("./data/atp/atp_fr.parquet")
        duckdb.sql("CREATE TABLE atp_fr AS SELECT * FROM 'data/atp/atp_fr.parquet'")
        return

    logger.info("Creating new atp_fr table and saving to parquet")
    duckdb.sql("""
        CREATE TABLE atp_fr AS
        SELECT
            properties->>'$.addr:country' as country,
            properties->>'$.addr:city' as city,
            properties->>'$.addr:postcode' as postcode,
            properties->>'$.brand:wikidata' as brand_wikidata,
            properties->>'$.brand' as brand,
            properties->>'$.name' as name,
            properties->>'$.opening_hours' as opening_hours,
            properties->>'$.website' as website,
            properties->>'$.phone' as phone,
            properties->>'$.email' as email,
            ST_AsGeoJSON(geom)
        FROM read_parquet('./data/atp/latest.parquet')
        WHERE properties->>'$.addr:country' = 'FR';
        CREATE INDEX atp_fr_brand_wikidata_idx ON atp_fr (brand_wikidata);
    """)
    duckdb.sql("COPY atp_fr TO './data/atp/atp_fr.parquet' (FORMAT parquet);")


@timer
def setup_osm_db(osmdb):
    cursor = osmdb.cursor()

    # Insert the EPSG/9793 official projection of France
    # See https://spatialreference.org/ref/epsg/9794/ and https://fr.wikipedia.org/wiki/Projection_conique_conforme_de_Lambert#Projections_officielles_en_France_m%C3%A9tropolitaine
    cursor.execute("SELECT * FROM spatial_ref_sys WHERE srid=9794;")
    spatial_refs = cursor.fetchall()
    if len(spatial_refs) == 0:
        logger.info("Insert EPSG/9794 projection into OSM database")
        cursor.execute("""
            INSERT INTO spatial_ref_sys (srid, auth_name, auth_srid, srtext, proj4text) 
            VALUES(9794, 'EPSG', 9794, 'PROJCS["RGF93_v2b_Lambert-93",GEOGCS["RGF93_v2b",DATUM["Reseau_Geodesique_Francais_1993_v2b",SPHEROID["GRS_1980",6378137.0,298.257222101]],PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]],PROJECTION["Lambert_Conformal_Conic"],PARAMETER["False_Easting",700000.0],PARAMETER["False_Northing",6600000.0],PARAMETER["Central_Meridian",3.0],PARAMETER["Standard_Parallel_1",49.0],PARAMETER["Standard_Parallel_2",44.0],PARAMETER["Latitude_Of_Origin",46.5],UNIT["Meter",1.0]]', '+proj=lcc +lat_0=46.5 +lon_0=3 +lat_1=49 +lat_2=44 +x_0=700000 +y_0=6600000 +ellps=GRS80 +units=m +no_defs +type=crs');
        """)

    if Config.force_osm_setup():
        logger.info("Force OSM setup")
        cursor.execute("DROP MATERIALIZED VIEW IF EXISTS mv_places CASCADE;")

    logger.info("Create Materialized View mv_places and associated indexes")
    cursor.execute("""
        CREATE MATERIALIZED VIEW IF NOT EXISTS mv_places AS
        SELECT
            node_id                  AS osm_id,
            'point'                  AS node_type,
            tags->>'name'            AS name,
            tags->>'brand:wikidata'  AS brand_wikidata,
            tags->>'brand'           AS brand,
            tags->>'addr:city'       AS city,
            tags->>'addr:postcode'   AS postcode,
            tags->>'opening_hours'   AS opening_hours,
            tags->>'website'         AS website,
            tags->>'phone'           AS phone,
            tags->>'email'           AS email,
            ST_Transform(geom, 9794) AS geom_9794,
            geom
        FROM points

        UNION ALL

        SELECT
            area_id                  AS osm_id,
            'polygon'                AS node_type,
            tags->>'name'            AS name,
            tags->>'brand:wikidata'  AS brand_wikidata,
            tags->>'brand'           AS brand,
            tags->>'addr:city'       AS city,
            tags->>'addr:postcode'   AS postcode,
            tags->>'opening_hours'   AS opening_hours,
            tags->>'website'         AS website,
            tags->>'phone'           AS phone,
            tags->>'email'           AS email,
            ST_Transform(geom, 9794) AS geom_9794,
            geom
        FROM polygons;

        -- 3.1  Index spatial (GIST) – indispensable pour ST_DWithin
        CREATE INDEX IF NOT EXISTS mv_places_geom_9794_idx
            ON mv_places USING GIST (geom_9794);

        -- 3.2  Index sur la clé brand:wikidata (exact match)
        CREATE INDEX IF NOT EXISTS mv_places_brand_wikidata_idx
            ON mv_places ((brand_wikidata));

        -- 3.3  Index fonctionnel insensible à la casse sur brand
        CREATE INDEX IF NOT EXISTS mv_places_brand_lower_idx
            ON mv_places (LOWER(brand));

        -- 3.4  Index fonctionnel insensible à la casse sur name
        CREATE INDEX IF NOT EXISTS mv_places_name_lower_idx
            ON mv_places (LOWER(name));

        -- 3.5  Index trigramme pour la similarité sur le nom
        CREATE INDEX IF NOT EXISTS mv_places_name_trgm_idx
            ON mv_places USING GIN (LOWER(name) gin_trgm_ops);

        -- 3.6  Normalisation du site web (supprime http/https) – insensible à la casse
        CREATE INDEX IF NOT EXISTS mv_places_website_norm_idx
            ON mv_places (LOWER(REGEXP_REPLACE(website, '^https?://', '', 'i')));

        -- 3.7  Normalisation du téléphone (supprime le préfixe +33 et les espaces)
        CREATE INDEX IF NOT EXISTS mv_places_phone_norm_idx
            ON mv_places (REGEXP_REPLACE(REGEXP_REPLACE(phone, '^\+33', '0'), '\s+', '', 'g'));

        -- 3.8  Index fonctionnel insensible à la casse sur l'email
        CREATE INDEX IF NOT EXISTS mv_places_email_lower_idx
            ON mv_places (LOWER(email));
    """)

    osmdb.commit()
    cursor.close()
    logger.info("OSM DB completely setup")
