from src.pipeline.atp import is_relevant_spider


def test_keeps_french_and_suffixless_spiders():
    assert is_relevant_spider("output/carrefour_fr.geojson")
    assert is_relevant_spider("ikea.geojson")
    assert is_relevant_spider("output/mcdonalds_eu.geojson")
    assert is_relevant_spider("output/some_brand.geojson")


def test_drops_foreign_spiders():
    assert not is_relevant_spider("output/aldi_de.geojson")
    assert not is_relevant_spider("starbucks_us.geojson")
    assert not is_relevant_spider("output/tesco_GB.geojson")
