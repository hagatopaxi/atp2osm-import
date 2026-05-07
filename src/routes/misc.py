import datetime
import logging

from io import BytesIO

from flask import Blueprint, render_template, request, Response
from psycopg.rows import dict_row
from staticmap import StaticMap, CircleMarker

from src.db import get_osmdb
from src.extensions import cache
from src.utils import fetch_osm_users

logger = logging.getLogger(__name__)

misc_bp = Blueprint("misc", __name__)

HISTORY_PER_PAGE = 20


@misc_bp.route("/")
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


@misc_bp.route("/history")
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


@misc_bp.route("/staticmap/<long>/<lat>")
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
