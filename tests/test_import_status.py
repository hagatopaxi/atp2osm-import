from src.utils import _determine_import_status


def r(*statuses):
    return [{"status": s} for s in statuses]


def test_all_success():
    assert _determine_import_status(r("success", "success")) == "success"


def test_all_failed():
    assert _determine_import_status(r("error_osm_api")) == "error"
    assert _determine_import_status(r("error_osm_api", "error_unknown")) == "error"


def test_mixed_is_partial():
    # le type d'erreur reste sur la ligne du département, pas ici
    assert _determine_import_status(r("success", "error_osm_api")) == "partial"
    assert _determine_import_status(r("success", "error_unknown")) == "partial"


def test_no_changeset_at_all():
    assert _determine_import_status([]) == "success"
