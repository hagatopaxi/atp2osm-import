import logging
from datetime import datetime, timezone, timedelta

from flask import Blueprint, render_template, request, abort
from psycopg.rows import dict_row

from src.db import get_osmdb
from src.matching import DEPARTEMENT_NAMES
from src.utils import HISTORY_FILTERS as FILTERS, build_filters, fetch_osm_users

logger = logging.getLogger(__name__)

history_bp = Blueprint("history", __name__)

HISTORY_PER_PAGE = 20

SORT_COLUMNS = {
    "date": "import_date",
    "brand": "brand_name",
    "status": "status",
    "user": "osm_user_id",
    "items": "items_count",
    "departements": "departements_count",
}


@history_bp.route("/history")
def history():
    osmdb = get_osmdb()
    page = max(1, request.args.get("page", 1, type=int))
    offset = (page - 1) * HISTORY_PER_PAGE
    where, params, filters = build_filters(request.args, FILTERS)
    sort = request.args.get("sort") if request.args.get("sort") in SORT_COLUMNS else "date"
    direction = "ASC" if request.args.get("dir") == "asc" else "DESC"

    with osmdb.cursor(row_factory=dict_row) as cursor:
        total = cursor.execute(
            f"SELECT COUNT(*) AS total FROM import_history {where}", params
        ).fetchone()["total"]

        entries = cursor.execute(
            f"""SELECT *,
                       (SELECT COUNT(*) FROM import_departements dpt
                        WHERE dpt.import_id = import_history.id) AS departements_count
                FROM import_history {where}
                ORDER BY {SORT_COLUMNS[sort]} {direction} NULLS LAST
                LIMIT %s OFFSET %s""",
            params + [HISTORY_PER_PAGE, offset],
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
        users=users,
        page=page,
        total_pages=total_pages,
        total=total,
        filters=filters,
        sort=sort,
        direction=direction.lower(),
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

        departements = cursor.execute(
            """SELECT * FROM import_departements
               WHERE import_id = %s ORDER BY departement_number""",
            (entry_id,),
        ).fetchall()

    if entry is None:
        abort(404)

    for dpt in departements:
        dpt["departement_name"] = DEPARTEMENT_NAMES.get(
            dpt["departement_number"], dpt["departement_number"]
        )

    users = fetch_osm_users([entry["osm_user_id"]])
    is_recent = (datetime.now(timezone.utc) - entry["import_date"]) < timedelta(minutes=5)
    # Taux de réussite en départements, seulement si le détail est connu :
    # les intégrations antérieures à la migration n'ont pas de lignes filles.
    success_rate = (
        (sum(1 for d in departements if d["status"] == "success"), len(departements))
        if departements
        else None
    )
    return render_template(
        "history_detail.html",
        entry=entry,
        departements=departements,
        success_rate=success_rate,
        users=users,
        is_recent=is_recent,
    )
