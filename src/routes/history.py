import logging
from datetime import datetime, timezone, timedelta

from flask import Blueprint, render_template, request, abort
from psycopg.rows import dict_row

from src.db import get_osmdb
from src.utils import build_filters, fetch_osm_users

logger = logging.getLogger(__name__)

history_bp = Blueprint("history", __name__)

HISTORY_PER_PAGE = 20

# Filters this page exposes, and the columns they apply to.
FILTERS = {
    "q": ("brand_name", "brand_wikidata"),
    "status": "status",
    "user": "osm_user_id",
    "date": "import_date",
}

WEEKLY_CHART_WEEKS = 26

# Weeks with no integration must still show as an empty bar, hence the series.
WEEKLY_CHART_SQL = """
    WITH weeks AS (
        SELECT generate_series(
            date_trunc('week', NOW()) - make_interval(weeks => %s - 1),
            date_trunc('week', NOW()),
            '1 week'
        )::date AS week
    )
    SELECT w.week, COALESCE(SUM(h.items_count), 0)::int AS count
    FROM weeks w
    LEFT JOIN import_history h
           ON date_trunc('week', h.import_date)::date = w.week
          {extra}
    GROUP BY w.week
    ORDER BY w.week
"""


@history_bp.route("/history")
def history():
    osmdb = get_osmdb()
    page = max(1, request.args.get("page", 1, type=int))
    offset = (page - 1) * HISTORY_PER_PAGE
    where, params, filters = build_filters(request.args, FILTERS)

    with osmdb.cursor(row_factory=dict_row) as cursor:
        total = cursor.execute(
            f"SELECT COUNT(*) AS total FROM import_history {where}", params
        ).fetchone()["total"]

        entries = cursor.execute(
            f"SELECT * FROM import_history {where} ORDER BY import_date DESC LIMIT %s OFFSET %s",
            params + [HISTORY_PER_PAGE, offset],
        ).fetchall()

        weekly = cursor.execute(
            WEEKLY_CHART_SQL.format(extra=where.replace("WHERE ", "AND ", 1)),
            [WEEKLY_CHART_WEEKS] + params,
        ).fetchall()

        all_user_ids = [
            r["osm_user_id"]
            for r in cursor.execute(
                "SELECT DISTINCT osm_user_id FROM import_history"
            ).fetchall()
        ]

    total_pages = max(1, -(-total // HISTORY_PER_PAGE))
    users = fetch_osm_users(all_user_ids)

    return render_template(
        "history.html",
        entries=entries,
        weekly=weekly,
        weekly_max=max((w["count"] for w in weekly), default=0),
        users=users,
        page=page,
        total_pages=total_pages,
        total=total,
        filters=filters,
        filter_users=sorted(
            ((uid, users.get(uid, str(uid))) for uid in all_user_ids),
            key=lambda u: u[1].lower(),
        ),
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
    is_recent = (datetime.now(timezone.utc) - entry["import_date"]) < timedelta(minutes=5)
    return render_template("history_detail.html", entry=entry, users=users, from_page=from_page, is_recent=is_recent)
