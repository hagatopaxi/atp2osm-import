import psycopg
from flask import g

from src.config import get_database


def maintenance_since(conn):
    """Return when the current maintenance started, or None if there is none.

    The marker is set by the pipeline when it starts and removed only when a
    run succeeds — a failed or interrupted run deliberately keeps the site in
    maintenance until an admin fixes it and relaunches the pipeline.
    See migrations/014_create_maintenance.sql for the manual override.
    """
    with conn.cursor() as cur:
        cur.execute("SELECT started_at FROM maintenance ORDER BY started_at LIMIT 1")
        row = cur.fetchone()
        return row[0] if row else None


def set_maintenance(conn, active):
    with conn.cursor() as cur:
        cur.execute("DELETE FROM maintenance")
        if active:
            cur.execute("INSERT INTO maintenance (started_at) VALUES (NOW())")
    conn.commit()


def get_osmdb():
    if "osmdb" not in g:
        osmdb = psycopg.connect(**get_database().connect_kwargs)
        g.osmdb = osmdb

    return g.osmdb


def teardown_osmdb(exception):
    osmdb = g.pop("osmdb", None)

    if osmdb is not None:
        osmdb.close()
