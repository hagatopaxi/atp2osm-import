import psycopg
from flask import g

from src.config import get_database


def maintenance_since(conn):
    """Return when the current maintenance started, or None if there is none.

    Maintenance is on while a datasource's *latest* row is 'pending': it posts
    one when it starts syncing and resolves it when it ends. An interrupted run
    leaves it pending, so the site stays in maintenance until the pipeline is
    relaunched — which supersedes the row on its own, nothing to clean up.
    See migrations/018_data_imports_pending.sql for the manual override.
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT MIN(created_at) FROM (
                SELECT DISTINCT ON (type) status, created_at
                FROM data_imports ORDER BY type, created_at DESC
            ) latest WHERE status = 'pending'
        """)
        return cur.fetchone()[0]


def get_osmdb():
    if "osmdb" not in g:
        osmdb = psycopg.connect(**get_database().connect_kwargs)
        g.osmdb = osmdb

    return g.osmdb


def teardown_osmdb(exception):
    osmdb = g.pop("osmdb", None)

    if osmdb is not None:
        osmdb.close()
