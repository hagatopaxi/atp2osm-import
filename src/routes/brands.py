import json
import logging
from math import ceil

from flask import (
    Blueprint,
    Response,
    redirect,
    render_template,
    request,
    session,
    url_for,
)
from psycopg.rows import dict_row
from requests_oauthlib import OAuth2Session

from src.db import get_osmdb
from src.extensions import cache
from src.matching import get_all, get_changes, get_filtered, get_stats
from src.routes.auth import auth_required
from src.upload import BulkUpload
from src.utils import get_rand_items

logger = logging.getLogger(__name__)

brands_bp = Blueprint("brands", __name__)


def _determine_import_status(errors: list[tuple[str, str]], has_changesets: bool) -> str:
    """Determine the import history status from typed errors and whether any changeset was created."""
    if not errors:
        return "success"
    error_types = {e[0] for e in errors}
    if has_changesets:
        return "partial_osm_api" if error_types == {"osm_api"} else "partial_unknown"
    return "error_osm_api" if error_types == {"osm_api"} else "error_unknown"


def get_changes_by_brand_wikidata(brand_wikidata):
    osmdb = get_osmdb()
    with osmdb.cursor(row_factory=dict_row) as cursor:
        get_filtered(cursor, brand=brand_wikidata)
        return get_changes(cursor)


@brands_bp.route("/brands")
# @cache.cached(key_prefix="brands")
def brands():
    osmdb = get_osmdb()
    metadata = get_all(osmdb)
    return render_template("brands.html", metadata=metadata, total_brands=len(metadata))


@brands_bp.route("/brands/<brand_wikidata>/validate")
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
    brand = items[0]["atp_brand"]
    for idx, item in enumerate(items):
        item["title"] = (
            f"{item['tag'].get('name') or item['atp_brand']} - {item['postcode']}"
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


@brands_bp.route("/brands/<brand_wikidata>/confirm")
@auth_required
def brands_confirm(brand_wikidata):
    changes = get_changes_by_brand_wikidata(brand_wikidata)

    if len(changes) == 0:
        return redirect(
            url_for("brands.brands_validate", brand_wikidata=brand_wikidata)
        )

    stats = get_stats(changes)

    return render_template(
        "brands/:brand_wikidata/confirm.html",
        stats=stats,
        logs=json.dumps(changes, indent=4, ensure_ascii=False),
    )


@brands_bp.route("/brands/<brand_wikidata>/rejected")
@auth_required
def brands_rejected(brand_wikidata):
    return render_template("brands/:brand_wikidata/rejected.html")


@brands_bp.route("/brands/<brand_wikidata>/report-error", methods=["POST"])
@auth_required
def report_error(brand_wikidata):
    data = request.get_json()
    comment = data.get("comment", "")
    brand_name = data.get("brand_name", "")
    osmdb = get_osmdb()
    with osmdb.cursor() as cursor:
        cursor.execute(
            """INSERT INTO import_history (brand_wikidata, osm_user_id, status, comment, brand_name)
               VALUES (%s, %s, 'cancelled', %s, %s) RETURNING id""",
            (brand_wikidata, session["user"]["osm_id"], comment, brand_name),
        )
        entry_id = cursor.fetchone()[0]
        osmdb.commit()
    return Response(json.dumps({"id": entry_id}), status=201, mimetype="application/json")


@brands_bp.route("/brands/<brand_wikidata>/upload", methods=["POST"])
@auth_required
def upload_changes(brand_wikidata):
    changes = get_changes_by_brand_wikidata(brand_wikidata)
    osm_session = OAuth2Session(token=session["token"])
    bulk_upload = BulkUpload(changes, session=osm_session)
    errors = bulk_upload.upload()
    bulk_upload.save_log_file()

    osmdb = get_osmdb()
    with osmdb.cursor() as cursor:
        status = _determine_import_status(errors, bool(bulk_upload.changesets))
        error_messages = [msg for _, msg in errors]

        if errors and not bulk_upload.changesets:
            cursor.execute(
                """INSERT INTO import_history (brand_wikidata, osm_user_id, status, comment, brand_name)
                   VALUES (%s, %s, %s, %s, %s) RETURNING id""",
                (
                    brand_wikidata,
                    session["user"]["osm_id"],
                    status,
                    "; ".join(error_messages),
                    bulk_upload.brand_name,
                ),
            )
            entry_id = cursor.fetchone()[0]
            osmdb.commit()
            return Response(
                json.dumps({"errors": error_messages, "id": entry_id}), status=422, mimetype="application/json"
            )
        elif errors and bulk_upload.changesets:
            cursor.execute(
                """INSERT INTO import_history (brand_wikidata, osm_user_id, status, comment, changeset_ids, brand_name)
                   VALUES (%s, %s, %s, %s, %s, %s) RETURNING id""",
                (
                    brand_wikidata,
                    session["user"]["osm_id"],
                    status,
                    "; ".join(error_messages),
                    bulk_upload.changesets,
                    bulk_upload.brand_name,
                ),
            )
            entry_id = cursor.fetchone()[0]
            osmdb.commit()
            return Response(
                json.dumps({"partial": True, "errors": error_messages, "id": entry_id}),
                status=200,
                mimetype="application/json",
            )
        else:
            stats = get_stats(changes)
            cursor.execute(
                """INSERT INTO import_history (brand_wikidata, osm_user_id, status, items_count, changeset_ids, brand_name, tags_count)
                   VALUES (%s, %s, 'success', %s, %s, %s, %s) RETURNING id""",
                (
                    brand_wikidata,
                    session["user"]["osm_id"],
                    len(changes),
                    bulk_upload.changesets,
                    bulk_upload.brand_name,
                    json.dumps(stats["by_tag"]),
                ),
            )
            entry_id = cursor.fetchone()[0]
            osmdb.commit()
            return Response(
                json.dumps({"id": entry_id}), status=200, mimetype="application/json"
            )
