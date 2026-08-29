import importlib.util
import pathlib

spec = importlib.util.spec_from_file_location(
    "calibrate_nsi_tags",
    pathlib.Path(__file__).parent.parent / "scripts" / "calibrate_nsi_tags.py",
)
calibrate = importlib.util.module_from_spec(spec)
spec.loader.exec_module(calibrate)


def nsi(*items):
    return {"nsi": {"brands/shop/supermarket": {"items": list(items)}}}


def item(**tags):
    return {"locationSet": {"include": ["fr"]}, "tags": {"brand:wikidata": "Q1", **tags}}


def test_keeps_every_tag_not_only_the_writable_ones():
    (row,) = calibrate.calibration_rows(nsi(item(brand="Lidl", cuisine="none")))
    assert row[5] == '{"brand:wikidata": "Q1", "brand": "Lidl", "cuisine": "none"}'


def test_drops_the_tags_a_group_disagrees_on_and_keeps_the_others():
    """Measuring a tag whose value depends on the item picked measures nothing."""
    (row,) = calibrate.calibration_rows(
        nsi(item(brand="Lidl", name="Lidl"), item(brand="Lidl", name="Lidl Drive"))
    )
    assert row[5] == '{"brand:wikidata": "Q1", "brand": "Lidl"}'


def test_a_tag_missing_from_one_item_of_the_group_is_a_disagreement():
    (row,) = calibrate.calibration_rows(nsi(item(brand="Lidl"), item()))
    assert row[5] == '{"brand:wikidata": "Q1"}'


def test_categories_stay_separate_groups():
    rows = calibrate.calibration_rows({"nsi": {
        "brands/shop/supermarket": {"items": [item(brand="A")]},
        "brands/amenity/fuel": {"items": [item(brand="B")]},
    }})
    assert sorted(row[4] for row in rows) == ["fuel", "supermarket"]


def test_regional_location_share_counts_the_country_own_regions():
    """fr-ara.geojson is not dropped, it is applied to the whole country."""
    assert calibrate.regional_location_share({"nsi": {"brands/shop/supermarket": {"items": [
        item(),
        {"locationSet": {"include": ["fr-ara.geojson"]}, "tags": {}},
        {"locationSet": {"include": ["fr-75"]}, "tags": {}},
        {"locationSet": {"include": ["us-tx.geojson"]}, "tags": {}},
        {"locationSet": {"include": ["001"]}, "tags": {}},
    ]}}}) == (2, 4)
