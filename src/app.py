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

env = os.getenv("APP_ENV")
if env is None:
    raise ValueError(
        "APP_ENV environment variable is required (DEVELOPMENT or PRODUCTION)"
    )
env = env.upper()
if env not in ("DEVELOPMENT", "PRODUCTION"):
    raise ValueError(f"APP_ENV must be DEVELOPMENT or PRODUCTION, got '{env}'")
logger.warning("*** Running in %s mode (OSM API: %s) ***", env, api_url)

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


@app.context_processor
def inject_globals():
    return {"api_url": api_url}


@app.teardown_appcontext
def teardown_osmdb(exception):
    osmdb = g.pop("osmdb", None)

    if osmdb is not None:
        osmdb.close()


@app.route("/")
# @cache.cached(key_prefix="home")
def home():
    osmdb = get_osmdb()
    with osmdb.cursor(row_factory=dict_row) as cursor:
        stats = cursor.execute("""
            SELECT
                COALESCE(SUM(items_count), 0) AS total_nodes_updated,
                COUNT(*) FILTER (WHERE status = 'success') AS successful_imports,
                COUNT(DISTINCT brand_wikidata) FILTER (WHERE status = 'success') AS brands_imported
            FROM import_history
        """).fetchone()
        data_imports = cursor.execute("""
            SELECT DISTINCT ON (type) type, date, status, created_at
            FROM data_imports
            ORDER BY type, created_at DESC
        """).fetchall()
    data_imports = {row["type"]: row for row in data_imports}
    return render_template("home.html", stats=stats, data_imports=data_imports)


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
        page=page,
        total_pages=total_pages,
    )


@app.route("/brands")
# @cache.cached(key_prefix="brands")
def brands():
    osmdb = get_osmdb()
    metadata = get_all(osmdb)
    return render_template("brands.html", metadata=metadata, total_brands=len(metadata))


@app.route("/brands/<brand_wikidata>/validate")
@auth_required
# @cache.cached(query_string=True, key_prefix="brands/")
def brands_validate(brand_wikidata):
    changes = get_changes_by_brand_wikidata(brand_wikidata)

    if len(changes) == 0:
        osmdb = get_osmdb()
        with osmdb.cursor() as cursor:
            brand_name = cursor.execute(
                "SELECT brand FROM atp_fr WHERE brand_wikidata = %s LIMIT 1",
                (brand_wikidata,),
            ).fetchone()
            brand_name = brand_name[0] if brand_name else None
            cursor.execute(
                """INSERT INTO import_history (brand_wikidata, osm_user_id, status, items_count, brand_name)
                   VALUES (%s, %s, 'success', 0, %s)""",
                (brand_wikidata, session["user"]["osm_id"], brand_name),
            )
            osmdb.commit()
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
        return redirect(url_for("brands_validate", brand_wikidata=brand_wikidata))

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

    osmdb = get_osmdb()
    with osmdb.cursor() as cursor:
        cursor.execute(
            """INSERT INTO import_history (brand_wikidata, osm_user_id, status, items_count, changeset_ids, brand_name)
               VALUES (%s, %s, 'success', %s, %s, %s)""",
            (
                brand_wikidata,
                session["user"]["osm_id"],
                len(changes),
                bulk_upload.changesets,
                bulk_upload.brand_name,
            ),
        )
        osmdb.commit()

    return Response(status=200)


@app.route("/todo")
def todo():
    osmdb = get_osmdb()
    with osmdb.cursor(row_factory=dict_row) as cursor:
        entries = cursor.execute(
            "SELECT * FROM todo_brands ORDER BY created_at DESC"
        ).fetchall()
    user_ids = list({e["osm_user_id"] for e in entries})
    users = fetch_osm_users(user_ids)
    return render_template("todo.html", entries=entries, users=users)


@app.route("/todo/check")
def todo_check():
    wikidata = request.args.get("wikidata", "").strip()
    name = request.args.get("name", "").strip()
    osmdb = get_osmdb()
    with osmdb.cursor(row_factory=dict_row) as cursor:
        matches = []
        if wikidata:
            row = cursor.execute(
                "SELECT id, brand_wikidata, brand_name FROM todo_brands WHERE brand_wikidata = %s",
                (wikidata,),
            ).fetchone()
            if row:
                matches.append(dict(row))
        if name and not matches:
            rows = cursor.execute(
                "SELECT id, brand_wikidata, brand_name FROM todo_brands WHERE brand_name ILIKE %s LIMIT 5",
                (f"%{name}%",),
            ).fetchall()
            matches.extend([dict(r) for r in rows])
    return {"matches": matches}


@app.route("/todo", methods=["POST"])
@auth_required
def todo_add():
    data = request.get_json()
    brand_wikidata = (data.get("brand_wikidata") or "").strip()
    brand_name = (data.get("brand_name") or "").strip()
    estimation = data.get("estimation")
    if estimation is not None:
        try:
            estimation = int(estimation)
        except (ValueError, TypeError):
            return {"error": "estimation doit être un entier"}, 400
    if not brand_wikidata or not brand_name:
        return {"error": "brand_wikidata et brand_name sont requis"}, 400
    osm_user_id = session["user"]["osm_id"]
    osmdb = get_osmdb()
    with osmdb.cursor(row_factory=dict_row) as cursor:
        try:
            cursor.execute(
                """INSERT INTO todo_brands (brand_wikidata, brand_name, osm_user_id, estimation)
                   VALUES (%s, %s, %s, %s)""",
                (brand_wikidata, brand_name, osm_user_id, estimation),
            )
            osmdb.commit()
        except psycopg.errors.UniqueViolation:
            osmdb.rollback()
            return {"error": "Cette marque est déjà dans la liste"}, 409
        except Exception:
            osmdb.rollback()
            logger.exception("Failed to insert todo brand")
            return {"error": "Une erreur est survenue, veuillez réessayer."}, 500
    return Response(status=201)


@app.route("/todo/<int:entry_id>", methods=["DELETE"])
@auth_required
def todo_delete(entry_id):
    osmdb = get_osmdb()
    with osmdb.cursor(row_factory=dict_row) as cursor:
        entry = cursor.execute(
            "SELECT osm_user_id FROM todo_brands WHERE id = %s", (entry_id,)
        ).fetchone()
        if entry is None:
            return Response(status=404)
        if entry["osm_user_id"] != session["user"]["osm_id"]:
            return abort(403)
        cursor.execute("DELETE FROM todo_brands WHERE id = %s", (entry_id,))
        osmdb.commit()
    return Response(status=204)


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


if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    app.run(port=port)
