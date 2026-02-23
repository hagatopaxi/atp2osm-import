import logging
import pathlib
import os
import psycopg
import datetime
import json

from io import BytesIO
from psycopg.rows import dict_row
from flask import (
    Flask,
    render_template,
    g,
    Response,
    request,
    session,
    url_for,
    redirect,
)
from flask_caching import Cache
from staticmap import StaticMap, CircleMarker
from math import ceil
from requests_oauthlib import OAuth2Session

from src.matching import get_all, get_filtered, get_changes, get_stats
from src.utils import get_rand_items


logger = logging.getLogger(__name__)

PROJECT_ROOT = pathlib.Path(__file__).parent.parent.resolve()
TEMPLATE_DIR = PROJECT_ROOT / "website" / "templates"
CACHE_DIR = PROJECT_ROOT / ".cache"
STATIC_DIR = PROJECT_ROOT / "static"

app = Flask(__name__, template_folder=TEMPLATE_DIR, static_folder=STATIC_DIR)
app.secret_key = os.urandom(24)

app.config["CACHE_TYPE"] = "FileSystemCache"
app.config["CACHE_DIR"] = CACHE_DIR
app.config["CACHE_THRESHOLD"] = 1000
app.config["CACHE_DEFAULT_TIMEOUT"] = 0  # Infinite cache duration

cache = Cache(app)


client_id = os.getenv("OSM_OAUTH_CLIENT_ID")
client_secret = os.getenv("OSM_OAUTH_CLIENT_SECRET")
api_url = os.getenv("OSM_API_HOST").strip("/")
authorization_base_url = f"{api_url}/oauth2/authorize"
token_url = f"{api_url}/oauth2/token"
scope = ["write_api"]


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


def get_changes_by_brand_wikidata(brand_wikidata):
    osmdb = get_osmdb()
    with osmdb.cursor(row_factory=dict_row) as cursor:
        get_filtered(cursor, brand=brand_wikidata)
        return get_changes(cursor)


@app.teardown_appcontext
def teardown_osmdb(exception):
    osmdb = g.pop("osmdb", None)

    if osmdb is not None:
        osmdb.close()


@app.route("/")
# @cache.cached(key_prefix="home")
def home():
    return render_template("home.html")


@app.route("/brands")
# @cache.cached(key_prefix="brands")
def brands():
    osmdb = get_osmdb()
    metadata = get_all(osmdb)
    return render_template("brands.html", metadata=metadata)


@app.route("/brands/<brand_wikidata>/validate")
# @cache.cached(query_string=True, key_prefix="brands/")
def brands_validate(brand_wikidata):
    changes = get_changes_by_brand_wikidata(brand_wikidata)

    if len(changes) == 0:
        return render_template("brands/:brand_wikidata/empty.html")

    # Check at least 5 items
    min_to_check = max(ceil(len(changes) / 100), 5)
    items = get_rand_items(changes, n=min_to_check)
    brand = items[0]["tag"]["brand"]
    for idx, item in enumerate(items):
        item["title"] = (
            f"{item['tag']['name'] if 'name' in item['tag'] else brand} - {item['postcode']}"
        )
        item["new_tags_keys"] = [
            key for key in item["tag"] if key not in item["old_tag"]
        ]

    return render_template(
        "brands/:brand_wikidata/validate.html",
        brand_wikidata=brand_wikidata,
        brand=brand,
        size=len(changes),
        items=items,
    )


@app.route("/brands/<brand_wikidata>/confirm")
def brands_confirm(brand_wikidata):
    changes = get_changes_by_brand_wikidata(brand_wikidata)

    if len(changes) == 0:
        return render_template("brands/:brand_wikidata/empty.html")

    stats = get_stats(changes)

    return render_template(
        "brands/:brand_wikidata/confirm.html",
        stats=stats,
        logs=json.dumps(changes, indent=4, ensure_ascii=False),
    )


@app.route("/brands/<brand_wikidata>/upload", methods=["POST"])
def upload_changes(brand_wikidata):
    # changes = get_changes_by_brand_wikidata(brand_wikidata)
    # bulk_upload = BulkUpload()
    return Response(status=200)


@app.route("/login", methods=["POST"])
def login():

    redirect_uri = url_for("oauth_callback", _external=True)

    osm = OAuth2Session(client_id, redirect_uri=redirect_uri, scope=scope)
    authorization_url, state = osm.authorization_url(authorization_base_url)
    session["oauth_state"] = state

    return authorization_url


@app.route("/oauth-callback")
def oauth_callback():
    if "error" in request.args:
        return "Authentication failed: " + request.args["error"], 401

    # Validate state
    if request.args.get("state") != session.get("oauth_state"):
        return "Invalid state parameter", 401

    redirect_uri = url_for("oauth_callback", _external=True)

    osm = OAuth2Session(
        client_id, redirect_uri=redirect_uri, state=session["oauth_state"]
    )

    token = osm.fetch_token(
        token_url, client_secret=client_secret, authorization_response=request.url
    )

    session.token = token

    return redirect("/")


@app.route("/staticmap/<long>/<lat>")
@cache.cached(query_string=True, key_prefix="staticmap/", timeout=300)
def staticmap(long, lat):
    m = StaticMap(400, 300, url_template="http://b.tile.osm.org/{z}/{x}/{y}.png")

    marker_outline = CircleMarker((float(long), float(lat)), "white", 18)
    marker = CircleMarker((float(long), float(lat)), "#0036FF", 12)

    m.add_marker(marker_outline)
    m.add_marker(marker)
    datetime.time()
    image = m.render(zoom=17)

    # In memory image returned directly to the client
    img_io = BytesIO()
    image.save(img_io, "PNG")
    img_io.seek(0)
    return Response(img_io, mimetype="image/png")


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
