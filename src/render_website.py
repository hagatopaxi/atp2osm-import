import psycopg
import os
import json
import pathlib
import datetime
import logging
import argparse
import sys

from jinja2 import Environment, FileSystemLoader, select_autoescape
from psycopg.rows import dict_row
from models import Config, ServerWrapper


logger = logging.getLogger(__name__)

PROJECT_ROOT = pathlib.Path(__file__).parent.parent.resolve()
TEMPLATE_DIR = PROJECT_ROOT / "website" / "templates"
OUTPUT_FILE = PROJECT_ROOT / "dist" / "index.html"


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

    return brands


def render_website(metadata: list):
    env = Environment(
        loader=FileSystemLoader(str(TEMPLATE_DIR)),
        autoescape=select_autoescape(["html", "xml"]),
        trim_blocks=True,
        lstrip_blocks=True,
    )

    template = env.get_template("metadata.html")

    rendered = template.render(metadata=metadata, now=datetime.datetime.utcnow())

    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

    with open(OUTPUT_FILE, "w") as file:
        file.write(rendered)

    logger.info(f"✅ Generated at {OUTPUT_FILE}")


def main(osmdb):
    """
    This function is to compile metadata file and deploy on a distant server.
    """

    parser = argparse.ArgumentParser(
        prog="atp2osm-import", description="Import ATP FR data into OSM"
    )
    parser.add_argument(
        "--dry",
        action="store_true",
        help="Compute changes without applying",
    )
    parser.add_argument(
        "-d",
        "--debug",
        action="store_true",
        help="Enable debug mode, that will slow down the process, better to use with filter",
    )
    args = parser.parse_args()
    Config.setup(args)

    logging.basicConfig(
        stream=sys.stdout, level=logging.DEBUG if Config.debug() else logging.INFO
    )

    metadata = create_metadata()
    render_website(metadata)

    server = ServerWrapper()
    server.clean_www()
    server.sync_file(
        src=pathlib.Path("./logs/metadata.json"),
        dest=pathlib.Path("logs"),
        recursive=True,
    )
    server.sync_file(
        src=pathlib.Path("./dist/index.html"), dest=pathlib.Path("www/"), recursive=True
    )


if __name__ == "__main__":
    with psycopg.connect(
        dbname=os.getenv("OSM_DB_NAME"),
        user=os.getenv("OSM_DB_USER"),
        password=os.getenv("OSM_DB_PASSWORD"),
        host=os.getenv("OSM_DB_HOST"),
        port=os.getenv("OSM_DB_PORT"),
    ) as osmdb:
        main(osmdb)
