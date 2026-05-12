import logging

from flask import Blueprint, render_template, request, abort
from psycopg.rows import dict_row

from src.db import get_osmdb
from src.utils import fetch_osm_users

logger = logging.getLogger(__name__)

history_bp = Blueprint("history", __name__)

HISTORY_PER_PAGE = 20


@history_bp.route("/history")
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


@history_bp.route("/history/<int:entry_id>")
def history_detail(entry_id):
    osmdb = get_osmdb()
    with osmdb.cursor(row_factory=dict_row) as cursor:
        entry = cursor.execute(
            "SELECT * FROM import_history WHERE id = %s", (entry_id,)
        ).fetchone()

    if entry is None:
        abort(404)

    from_page = request.args.get("page", 1, type=int)
    users = fetch_osm_users([entry["osm_user_id"]])
    return render_template("history_detail.html", entry=entry, users=users, from_page=from_page)
