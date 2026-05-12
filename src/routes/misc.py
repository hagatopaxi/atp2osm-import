import datetime
import logging

from io import BytesIO

from flask import Blueprint, render_template, Response
from psycopg.rows import dict_row
from staticmap import StaticMap, CircleMarker

from src.db import get_osmdb
from src.extensions import cache

logger = logging.getLogger(__name__)

misc_bp = Blueprint("misc", __name__)


@misc_bp.route("/")
# @cache.cached(key_prefix="home")
def home():
    osmdb = get_osmdb()
    with osmdb.cursor(row_factory=dict_row) as cursor:
        stats = cursor.execute("""
            SELECT
                COALESCE(SUM(items_count), 0) AS total_nodes_updated,
                COUNT(*) FILTER (WHERE status = 'success') AS successful_imports,
                COUNT(DISTINCT brand_wikidata) FILTER (WHERE status = 'success') AS brands_imported,
                COALESCE(SUM((tags_count->>'opening_hours')::int), 0) AS opening_hours_added,
                COALESCE(SUM((tags_count->>'phone')::int), 0) AS phone_added,
                COALESCE(SUM((tags_count->>'website')::int), 0) AS website_added,
                COALESCE(SUM((tags_count->>'email')::int), 0) AS email_added
            FROM import_history
        """).fetchone()
        data_imports = cursor.execute("""
            SELECT DISTINCT ON (type) type, date, status, created_at
            FROM data_imports
            ORDER BY type, created_at DESC
        """).fetchall()
    data_imports = {row["type"]: row for row in data_imports}
    return render_template("home.html", stats=stats, data_imports=data_imports)


@misc_bp.route("/docs")
def docs():
    return render_template("docs.html")


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
