import psycopg
import os
import json

from psycopg.rows import dict_row
from utils import sync_file


def create_metadata():
    query = "SELECT brand_wikidata, (select brand from atp_fr af_2 where af_2.brand_wikidata = af.brand_wikidata limit 1) as brand, count(*) FROM atp_fr af WHERE brand_wikidata is not NULL and brand is not NULL group BY brand_wikidata order by brand_wikidata"

    with osmdb.cursor(row_factory=dict_row) as cursor:
        brands = cursor.execute(query).fetchall()

    for brand in brands:
        log_path = f"./logs/{brand['brand_wikidata']}"

        if os.path.isdir(log_path):
            files = os.listdir(log_path)
            if len(files) == 0:
                brand["last_import"] = "never"

            # File names are formatted as YYYY-MM-DD.json
            brand["last_import"] = files[0].split(".")[0]
        else:
            brand["last_import"] = "never"

    brands = sorted(brands, key=lambda brand: brand["last_import"], reverse=True)
    with open("./logs/metadata.json", "w") as file:
        file.write(json.dumps(brands, indent=4))


def main(osmdb):
    """
    This function is to compile metadata file and deploy on a distant server.
    """
    create_metadata()
    sync_file("./logs/metadata.json")


if __name__ == "__main__":
    with psycopg.connect(
        dbname=os.getenv("OSM_DB_NAME"),
        user=os.getenv("OSM_DB_USER"),
        password=os.getenv("OSM_DB_PASSWORD"),
        host=os.getenv("OSM_DB_HOST"),
        port=os.getenv("OSM_DB_PORT"),
    ) as osmdb:
        main(osmdb)
