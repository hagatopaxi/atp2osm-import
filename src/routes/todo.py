import logging

import psycopg
from flask import Blueprint, render_template, request, session, abort, Response
from psycopg.rows import dict_row

from src.db import get_osmdb
from src.routes.auth import auth_required
from src.utils import (
    TODO_FILTERS as FILTERS,
    build_filters,
    fetch_osm_users,
    hide_brands_in_atp,
)

logger = logging.getLogger(__name__)

todo_bp = Blueprint("todo", __name__)

SORT_COLUMNS = {
    "name": "brand_name",
    "wikidata": "brand_wikidata",
    "estimation": "estimation",
    "user": "osm_user_id",
    "date": "created_at",
}


@todo_bp.route("/todo")
def todo():
    osmdb = get_osmdb()
    where, params, filters = build_filters(request.args, FILTERS)
    where = hide_brands_in_atp(where, request.args, filters)
    sort = request.args.get("sort") if request.args.get("sort") in SORT_COLUMNS else "date"
    direction = "ASC" if request.args.get("dir") == "asc" else "DESC"
    with osmdb.cursor(row_factory=dict_row) as cursor:
        entries = cursor.execute(
            f"SELECT * FROM todo_brands {where} "
            f"ORDER BY {SORT_COLUMNS[sort]} {direction} NULLS LAST",
            params,
        ).fetchall()
        total = cursor.execute(
            "SELECT COUNT(*) AS total FROM todo_brands"
        ).fetchone()["total"]
        all_user_ids = [
            r["osm_user_id"]
            for r in cursor.execute(
                "SELECT DISTINCT osm_user_id FROM todo_brands"
            ).fetchall()
        ]

    users = fetch_osm_users(all_user_ids)
    current_user_id = session["user"]["osm_id"] if "user" in session else None
    return render_template(
        "todo.html",
        entries=entries,
        users=users,
        current_user_id=current_user_id,
        total=total,
        filters=filters,
        sort=sort,
        direction=direction.lower(),
        filter_users=sorted(
            ((uid, users.get(uid, str(uid))) for uid in all_user_ids),
            key=lambda u: u[1].lower(),
        ),
    )


@todo_bp.route("/todo/check")
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


@todo_bp.route("/todo", methods=["POST"])
@auth_required
def todo_add():
    data = request.get_json()
    brand_wikidata = (data.get("brand_wikidata") or "").strip() or None
    brand_name = (data.get("brand_name") or "").strip()
    estimation = data.get("estimation")
    if estimation is not None:
        try:
            estimation = int(estimation)
        except (ValueError, TypeError):
            return {"error": "estimation doit être un entier"}, 400
    if not brand_name:
        return {"error": "brand_name est requis"}, 400
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


@todo_bp.route("/todo/<int:entry_id>", methods=["DELETE"])
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
