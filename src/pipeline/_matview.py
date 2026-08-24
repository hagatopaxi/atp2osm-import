"""Rebuild-when-needed guard for materialized views.

A view's content depends on two things: the SQL that builds it, and the
datasources it reads. Both are folded into a signature stamped on the view
itself as a COMMENT, and compared before rebuilding.

This exists because the alternatives both fail:

* rebuilding every night costs a minute for nothing, 364 nights out of 365;
* guarding on the freshness of a single datasource silently freezes the view
  when *another* one moves — mv_places reads nsi_brands as well as the OSM
  tables, and mv_places_brand reads mv_places and atp_fr.

Listing a view's inputs is therefore not optional: an input left out is an
update that never lands. Whatever a view reads, pass it here.
"""

import hashlib

from psycopg import sql


def signature(view_sql: str, *inputs) -> str:
    """Signature of a view definition together with the state of its inputs.

    `inputs` is anything that identifies the version of what the view reads —
    an import date, a source version string. None is fine: it just means "no
    data yet", and differs from any later value.
    """
    parts = [view_sql, *(str(value) for value in inputs)]
    return hashlib.sha256("\0".join(parts).encode()).hexdigest()[:16]


def is_current(conn, name: str, sig: str) -> bool:
    """True when `name` exists and was built from this exact signature."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT obj_description(to_regclass(%s), 'pg_class')", (name,)
        )
        row = cur.fetchone()
    return bool(row) and row[0] == sig


def stamp(cur, name: str, sig: str) -> None:
    """Record the signature on the view, once it is built."""
    # COMMENT ON is a utility statement: it takes no bound parameter, so the
    # value has to be composed in.
    cur.execute(
        sql.SQL("COMMENT ON MATERIALIZED VIEW {} IS {}").format(
            sql.Identifier(name), sql.Literal(sig)
        )
    )
