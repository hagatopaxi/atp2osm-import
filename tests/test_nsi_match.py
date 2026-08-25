"""nsi_match() runs in PostgreSQL, so this test needs the real database."""

import psycopg
import pytest

QID = "Q-test-nsi-match"


@pytest.fixture
def conn():
    from src.config import ConfigError, get_database

    try:
        kwargs = get_database().connect_kwargs
    except ConfigError as exc:
        pytest.skip(f"no database configured: {exc}")
    try:
        c = psycopg.connect(**kwargs)
    except psycopg.OperationalError as exc:
        pytest.skip(f"no database available: {exc}")

    with c:
        c.execute(
            "INSERT INTO nsi_brands"
            " (brand_wikidata, brand, name, primary_key, primary_value, tags)"
            " VALUES (%s, 'Test', 'Test', 'amenity', 'fuel', %s)",
            (QID, '{"amenity": "fuel", "operator:wikidata": "Q-op"}'),
        )
        yield c
        c.rollback()  # nsi_brands is pipeline-owned, leave it untouched
    c.close()


def match(conn, tags):
    return conn.execute("SELECT nsi_match(%s::jsonb)", (tags,)).fetchone()[0]


def test_applies_to_an_object_of_the_same_category(conn):
    got = match(conn, '{"amenity": "fuel", "brand:wikidata": "%s"}' % QID)
    assert got == {"amenity": "fuel", "operator:wikidata": "Q-op"}


def test_applies_to_an_object_without_a_primary_tag(conn):
    got = match(conn, '{"brand:wikidata": "%s"}' % QID)
    assert got == {"amenity": "fuel", "operator:wikidata": "Q-op"}


def test_never_reclassifies_an_object_of_another_category(conn):
    # way/130021335: a Casino supermarket carrying the QID of Casino's fuel
    # stations must not come out as amenity=fuel.
    got = match(conn, '{"shop": "supermarket", "brand:wikidata": "%s"}' % QID)
    assert got == {"operator:wikidata": "Q-op"}


def test_redefining_the_function_invalidates_the_view_signature(conn):
    """The guard that makes a corrected nsi_match() actually reach mv_places."""
    from src.pipeline import _matview

    before = _matview.function_defs(conn, "nsi_match", "osm_primary_tag")
    conn.execute(
        "CREATE OR REPLACE FUNCTION nsi_match(osm_tags jsonb) RETURNS jsonb"
        " AS $$ SELECT NULL::jsonb $$ LANGUAGE sql STABLE"
    )
    after = _matview.function_defs(conn, "nsi_match", "osm_primary_tag")

    assert before != after
    assert _matview.signature("same view sql", before) != _matview.signature(
        "same view sql", after
    )
