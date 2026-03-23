import logging
import pathlib
import os
import psycopg
import datetime
import json
import functools
import requests

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
    abort,
    redirect,
)
from flask_caching import Cache
from staticmap import StaticMap, CircleMarker
from math import ceil
from requests_oauthlib import OAuth2Session

from src.matching import get_all, get_filtered, get_changes, get_stats
from src.utils import get_rand_items
from src.upload import BulkUpload
from src.migrate import run_migrations

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


@app.template_filter("parse_comment")
def parse_comment(value):
    try:
        return json.loads(value)
    except (json.JSONDecodeError, TypeError):
        return value


def run_startup_tasks():
    """Run migrations at server startup."""
    try:
        with psycopg.connect(
            dbname=os.getenv("OSM_DB_NAME"),
            user=os.getenv("OSM_DB_USER"),
            password=os.getenv("OSM_DB_PASSWORD"),
            host=os.getenv("OSM_DB_HOST"),
            port=os.getenv("OSM_DB_PORT"),
        ) as conn:
            run_migrations(conn)
    except Exception:
        logger.exception("Startup tasks failed.")
        raise


run_startup_tasks()


client_id = os.getenv("OSM_OAUTH_CLIENT_ID")
client_secret = os.getenv("OSM_OAUTH_CLIENT_SECRET")
api_url = os.getenv("OSM_API_HOST").strip("/")
authorization_base_url = f"{api_url}/oauth2/authorize"
token_url = f"{api_url}/oauth2/token"
scope = ["write_api", "read_prefs"]

# Save token in memory, indexed by osm user's id
token_store = {}


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


def auth_required(f):
    @functools.wraps(f)
    def decorator(*args, **kwargs):
        if "user" in session:
            return f(*args, **kwargs)
        else:
            return abort(403)

    return decorator


@app.teardown_appcontext
def teardown_osmdb(exception):
    osmdb = g.pop("osmdb", None)

    if osmdb is not None:
        osmdb.close()


@app.route("/")
# @cache.cached(key_prefix="home")
def home():
    return render_template("home.html")


HISTORY_PER_PAGE = 20


def fetch_osm_users(user_ids):
    """Batch fetch user display names from the OSM API."""
    if not user_ids:
        return {}
    ids_param = ",".join(str(uid) for uid in user_ids)
    try:
        resp = requests.get(
            f"{api_url}/api/0.6/users.json?users={ids_param}",
            timeout=5,
        )
        resp.raise_for_status()
        data = resp.json()
        return {
            u["user"]["id"]: u["user"]["display_name"] for u in data.get("users", [])
        }
    except Exception:
        logger.exception("Failed to fetch OSM user details")
        return {}


@app.route("/history")
def history():
    osmdb = get_osmdb()
    page = request.args.get("page", 1, type=int)
    page = max(1, page)
    offset = (page - 1) * HISTORY_PER_PAGE

    with osmdb.cursor(row_factory=dict_row) as cursor:
        cursor.execute("SELECT COUNT(*) AS total FROM import_history")
        total = cursor.fetchone()["total"]

        cursor.execute(
            "SELECT * FROM import_history ORDER BY import_date DESC LIMIT %s OFFSET %s",
            (HISTORY_PER_PAGE, offset),
        )
        entries = cursor.fetchall()

    total_pages = max(1, -(-total // HISTORY_PER_PAGE))

    user_ids = list({e["osm_user_id"] for e in entries})
    users = fetch_osm_users(user_ids)

    return render_template(
        "history.html",
        entries=entries,
        users=users,
        api_url=api_url,
        page=page,
        total_pages=total_pages,
    )


@app.route("/brands")
# @cache.cached(key_prefix="brands")
def brands():
    osmdb = get_osmdb()
    metadata = get_all(osmdb)
    return render_template("brands.html", metadata=metadata)


@app.route("/brands/<brand_wikidata>/validate")
@auth_required
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
@auth_required
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


@app.route("/brands/<brand_wikidata>/rejected")
@auth_required
def brands_rejected(brand_wikidata):
    return render_template("brands/:brand_wikidata/rejected.html")


@app.route("/brands/<brand_wikidata>/report-error", methods=["POST"])
@auth_required
def report_error(brand_wikidata):
    data = request.get_json()
    comment = data.get("comment", "")
    brand_name = data.get("brand_name", "")
    osmdb = get_osmdb()
    with osmdb.cursor() as cursor:
        cursor.execute(
            """INSERT INTO import_history (brand_wikidata, osm_user_id, status, comment, brand_name)
               VALUES (%s, %s, 'error', %s, %s)""",
            (brand_wikidata, session["user"]["osm_id"], comment, brand_name),
        )
        osmdb.commit()
    return Response(status=201)


@app.route("/brands/<brand_wikidata>/upload", methods=["POST"])
@auth_required
def upload_changes(brand_wikidata):
    changes = get_changes_by_brand_wikidata(brand_wikidata)
    osm_session = OAuth2Session(token=token_store[session["user"]["osm_id"]])
    bulk_upload = BulkUpload(changes, session=osm_session)
    bulk_upload.upload()
    bulk_upload.save_log_file()
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
    user_detail_url = f"{api_url}/api/0.6/user/details.json"
    response = osm.get(user_detail_url)
    res_json = response.json()
    user = {"osm_id": res_json["user"]["id"], "name": res_json["user"]["display_name"]}
    token_store[user["osm_id"]] = token

    del session["oauth_state"]
    session["user"] = user

    return redirect("/")


@app.route("/logout", methods=["POST"])
@auth_required
def logout():
    user_id = session["user"]["osm_id"]

    # remove the saved token
    if user_id in token_store:
        del token_store[session["user"]["osm_id"]]

    # clean the session
    session.clear()

    return Response(200)


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


@app.errorhandler(403)
def not_authorized_error(error):
    return render_template("errors/403.html"), 403


@app.errorhandler(404)
def not_found_error(error):
    return render_template("errors/404.html"), 404


# Default port:
if __name__ == "__main__":
    app.run()
