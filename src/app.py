import logging
import pathlib
import os
import psycopg

from flask import Flask, render_template, g
from flask_caching import Cache
from src.views_utils import get_metadata


logger = logging.getLogger(__name__)

PROJECT_ROOT = pathlib.Path(__file__).parent.parent.resolve()
TEMPLATE_DIR = PROJECT_ROOT / "website" / "templates"

app = Flask(__name__, template_folder=TEMPLATE_DIR)

app.config["CACHE_TYPE"] = "FileSystemCache"
app.config["CACHE_DIR"] = "./.cache"
app.config["CACHE_THRESHOLD"] = 1000
app.config["CACHE_DEFAULT_TIMEOUT"] = 0  # Infinite cache duration

cache = Cache(app)


def get_osmdb():
    if "osmdb" not in g:
        osmdb = psycopg.connect(
            dbname=os.getenv("OSM_DB_NAME"),
            user=os.getenv("OSM_DB_USER"),
            password=os.getenv("OSM_DB_PASSWORD"),
            host=os.getenv("OSM_DB_HOST"),
            port=os.getenv("OSM_DB_PORT"),
        )
        g.osmdb = osmdb

    return g.osmdb


@app.teardown_appcontext
def teardown_osmdb(exception):
    osmdb = g.pop("osmdb", None)

    if osmdb is not None:
        osmdb.close()


@app.route("/")
@cache.cached(key_prefix="brands")
def home():
    return render_template("home.html")


@app.route("/brands")
@cache.cached(key_prefix="brands")
def brands():
    osmdb = get_osmdb()
    metadata = get_metadata(osmdb)
    return render_template("brands.html", metadata=metadata)


@app.route("/invalidate/<key>")
def invalidate_cache(key):
    cache.delete(key)
    return "OK"


@app.errorhandler(500)
def internal_error(error):
    return render_template("errors/500.html"), 500


@app.errorhandler(404)
def not_found_error(error):
    return render_template("errors/404.html"), 404


# Default port:
if __name__ == "__main__":
    app.run()
