"""Needs the local PostGIS (podman-compose up -d); skipped otherwise."""
import pathlib

import psycopg
import pytest

from src.config import ConfigError, get_database
from src.db import maintenance_since
from src.pipeline._db import record_import, start_import
from src.pipeline.dag import record_failure

MIGRATION = pathlib.Path(__file__).parent.parent / "migrations" / "018_data_imports_pending.sql"


@pytest.fixture
def conn():
    try:
        with psycopg.connect(**get_database().connect_kwargs) as c:
            c.execute(MIGRATION.read_text())
            c.execute("DELETE FROM data_imports WHERE type = 'pipeline'")
            c.commit()
            yield c
            c.execute("DELETE FROM data_imports WHERE type = 'pipeline'")
            c.commit()
    except (psycopg.OperationalError, ConfigError) as exc:
        pytest.skip(f"no database available: {exc}")


def test_pending_row_holds_maintenance_until_resolved(conn):
    assert maintenance_since(conn) is None

    start_import(conn, "pipeline")
    started = maintenance_since(conn)
    assert started is not None

    # A new process (crashed run, restarted app) still sees the pending row.
    with psycopg.connect(**get_database().connect_kwargs) as other:
        assert maintenance_since(other) == started

    # record_import resolves the pending row instead of stacking a new one.
    record_import(conn, "pipeline", None, "success")
    assert maintenance_since(conn) is None
    count = conn.execute("SELECT COUNT(*) FROM data_imports WHERE type='pipeline'").fetchone()
    assert count[0] == 1


def test_a_failing_step_closes_the_open_row_instead_of_stacking_one(conn):
    start_import(conn, "pipeline")

    record_failure("pipeline-whatever", RuntimeError("boom"))

    row = conn.execute(
        "SELECT status, comment FROM data_imports WHERE type='pipeline'"
    ).fetchall()
    assert len(row) == 1
    assert row[0][0] == "pending"  # still in maintenance
    assert "boom" in row[0][1]


def test_a_relaunch_supersedes_the_row_a_crashed_run_left_behind(conn):
    start_import(conn, "pipeline")  # crashes: never resolved
    assert maintenance_since(conn) is not None

    start_import(conn, "pipeline")
    record_import(conn, "pipeline", None, "success")

    # Only the latest row counts, so the stale one needs no cleanup.
    assert maintenance_since(conn) is None
