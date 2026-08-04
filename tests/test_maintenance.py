"""Needs the local PostGIS (podman-compose up -d); skipped otherwise."""
import pathlib

import psycopg
import pytest

from src.config import ConfigError, get_database
from src.db import maintenance_since, set_maintenance

MIGRATION = pathlib.Path(__file__).parent.parent / "migrations" / "014_create_maintenance.sql"


@pytest.fixture
def conn():
    try:
        with psycopg.connect(**get_database().connect_kwargs) as c:
            c.execute(MIGRATION.read_text())
            c.commit()
            yield c
            set_maintenance(c, False)
    except (psycopg.OperationalError, ConfigError) as exc:
        pytest.skip(f"no database available: {exc}")


def test_marker_survives_until_explicitly_cleared(conn):
    set_maintenance(conn, False)
    assert maintenance_since(conn) is None

    set_maintenance(conn, True)
    started = maintenance_since(conn)
    assert started is not None

    # A new process (crashed run, restarted app) still sees the marker.
    with psycopg.connect(**get_database().connect_kwargs) as other:
        assert maintenance_since(other) == started

    set_maintenance(conn, False)
    assert maintenance_since(conn) is None
