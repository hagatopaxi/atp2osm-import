"""Open export API for the data atp2osm itself produces.

Only the tables written by the app are exposed: integration history, its
per-département detail, and the brands-to-do list. Nothing coming from ATP or
OSM (or derived from them) is exported — those upstreams publish their own data.
"""

import csv
import json
import logging
from io import StringIO

from flask import Blueprint, Response, request
from psycopg.rows import dict_row

from src.db import get_osmdb
from src.utils import HISTORY_FILTERS, TODO_FILTERS, build_filters, hide_brands_in_atp

logger = logging.getLogger(__name__)

export_bp = Blueprint("export", __name__)

# dataset name -> (columns, table, filter spec, ORDER BY)
DATASETS = {
    "history": (
        "id, brand_wikidata, brand_name, osm_user_id, import_date, status, "
        "items_count, tags_count",
        "import_history",
        HISTORY_FILTERS,
        "import_date DESC",
    ),
    "departements": (
        "id, import_id, departement_number, items_count, osm_changeset_id, status, comment",
        "import_departements",
        {},
        "import_id DESC, departement_number",
    ),
    "todo": (
        "id, brand_wikidata, brand_name, osm_user_id, created_at, estimation",
        "todo_brands",
        TODO_FILTERS,
        "created_at DESC",
    ),
}

# ponytail: no pagination — these tables are in the thousands of rows at most.
# Add a LIMIT/cursor if they ever grow past what a single response can hold.


@export_bp.route("/api/export/<dataset>.<fmt>")
def export(dataset, fmt):
    if dataset not in DATASETS or fmt not in ("json", "csv"):
        return {"error": "Jeu de données ou format inconnu"}, 404

    columns, table, filter_spec, order_by = DATASETS[dataset]
    where, params, _ = build_filters(request.args, filter_spec)
    if dataset == "todo":
        where = hide_brands_in_atp(where, request.args)

    with get_osmdb().cursor(row_factory=dict_row) as cursor:
        rows = cursor.execute(
            f"SELECT {columns} FROM {table} {where} ORDER BY {order_by}", params
        ).fetchall()

    if fmt == "json":
        body = json.dumps(rows, default=str, ensure_ascii=False)
        mimetype = "application/json"
    else:
        buffer = StringIO()
        field_names = [c.strip() for c in columns.split(",")]
        writer = csv.DictWriter(buffer, fieldnames=field_names)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: _csv_value(v) for k, v in row.items()})
        body = buffer.getvalue()
        mimetype = "text/csv"

    return Response(
        body,
        mimetype=mimetype,
        headers={
            "Content-Disposition": f'attachment; filename="atp2osm-{dataset}.{fmt}"',
            "Access-Control-Allow-Origin": "*",  # open API: usable from any client
        },
    )


def _csv_value(value):
    """Flatten the values CSV has no cell type for (JSON objects, arrays)."""
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False)
    return value
