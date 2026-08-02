from datetime import datetime

from werkzeug.datastructures import MultiDict

from src.routes.history import FILTERS as HISTORY_FILTERS
from src.utils import build_filters, filter_brands


def test_no_filters():
    assert build_filters(MultiDict(), HISTORY_FILTERS) == ("", [], {})


def test_all_filters():
    where, params, active = build_filters(
        MultiDict(
            {
                "q": "carre",
                "status": "partial",
                "user": "42",
                "from": "2026-01-01",
                "to": "2026-02-01",
            }
        ),
        HISTORY_FILTERS,
    )
    assert where.startswith("WHERE ")
    assert where.count(" AND ") == 4
    assert params == [
        "%carre%",
        "%carre%",
        ["partial_osm_api", "partial_unknown"],
        42,
        "2026-01-01",
        "2026-02-01",
    ]
    assert active["status"] == "partial" and active["user"] == 42


def test_unknown_status_and_blanks_ignored():
    assert build_filters(MultiDict({"status": "bogus", "q": "  ", "to": ""}), HISTORY_FILTERS) == (
        "",
        [],
        {},
    )


def _brand(name, wikidata, total, status=None, last_import=None):
    return {
        "brand": name,
        "brand_wikidata": wikidata,
        "total": total,
        "last_status": status,
        "last_import": last_import,
    }


BRANDS = [
    _brand("Carrefour", "Q217599", 10),
    _brand("Lidl", "Q151954", 500, "success", datetime(2026, 3, 1)),
    _brand("Aldi", "Q125054", 20, "error_osm_api", datetime(2026, 1, 15)),
]


def test_brands_scope_importable_is_the_default():
    rows, active = filter_brands(BRANDS, MultiDict(), 50)
    assert [r["brand"] for r in rows] == ["Carrefour", "Aldi"]
    assert active == {"scope": "importable"}


def test_brands_scope_all_keeps_oversized_brands():
    rows, _ = filter_brands(BRANDS, MultiDict({"scope": "all"}), 50)
    assert len(rows) == 3


def test_brands_search_status_and_dates():
    rows, _ = filter_brands(BRANDS, MultiDict({"scope": "all", "q": "ald"}), 50)
    assert [r["brand"] for r in rows] == ["Aldi"]

    rows, _ = filter_brands(BRANDS, MultiDict({"scope": "all", "status": "error"}), 50)
    assert [r["brand"] for r in rows] == ["Aldi"]

    # jamais intégrée
    rows, _ = filter_brands(BRANDS, MultiDict({"scope": "all", "status": "none"}), 50)
    assert [r["brand"] for r in rows] == ["Carrefour"]

    rows, _ = filter_brands(BRANDS, MultiDict({"scope": "all", "from": "2026-02-01"}), 50)
    assert [r["brand"] for r in rows] == ["Lidl"]


# Missing-brands page config (importing src.routes.todo would pull the app config).
TODO_FILTERS = {"q": ("brand_name", "brand_wikidata"), "user": "osm_user_id", "date": "created_at"}


def test_filter_absent_from_spec_is_ignored():
    """The missing-brands page exposes no status: the parameter has no effect."""
    where, params, active = build_filters(
        MultiDict({"status": "success", "user": "7"}), TODO_FILTERS
    )
    assert where == "WHERE osm_user_id = %s"
    assert params == [7]
    assert active == {"user": 7}


def test_spec_drives_the_columns():
    where, _, _ = build_filters(MultiDict({"from": "2026-01-01"}), TODO_FILTERS)
    assert where == "WHERE created_at >= %s"
