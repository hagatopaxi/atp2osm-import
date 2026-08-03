"""Répétition générale de la reprise (migration 016) sur les données de prod.

    uv run --env-file .env python scripts/check_backfill.py \
        data/prod-history.sql data/prod-logs [--keep]

Charge le dump d'import_history dans une base jetable, applique 015 puis 016,
imprime un rapport et ne touche à rien d'autre. Avec --keep, la base est
conservée pour être inspectée à la main ; sans, elle est supprimée. Le rapport dit ce qui a été détaillé, ce qui ne l'a pas été et pourquoi,
et surtout les écarts entre l'historique existant et ce que la reprise en
déduit.
"""

import collections
import importlib.util
import os
import pathlib
import subprocess
import sys

import psycopg

sys.path.insert(0, str(pathlib.Path(__file__).parent.parent))

from src.config import get_database  # noqa: E402
from src.utils import _determine_import_status  # noqa: E402

MIGRATIONS = pathlib.Path(__file__).parent.parent / "migrations"
CHECK_DB = "atp2osm_backfill_check"


def psql(kwargs, sql: str):
    """Joue du SQL sans jamais mettre le mot de passe dans une ligne de commande."""
    env = {**os.environ, "PGPASSWORD": kwargs["password"]}
    cmd = ["psql", "-v", "ON_ERROR_STOP=1", "-q",
           "-h", kwargs["host"], "-p", str(kwargs["port"]),
           "-U", kwargs["user"], "-d", kwargs["dbname"], "-f", "-"]
    done = subprocess.run(cmd, input=sql, text=True, env=env)
    if done.returncode:
        raise SystemExit(f"psql a échoué (code {done.returncode})")


def main(dump, logs_dir, keep=False):
    db = get_database()
    admin = {**db.connect_kwargs, "dbname": "postgres", "autocommit": True}
    with psycopg.connect(**admin) as conn:
        conn.execute(f'DROP DATABASE IF EXISTS "{CHECK_DB}"')
        conn.execute(f'CREATE DATABASE "{CHECK_DB}"')

    kwargs = {**db.connect_kwargs, "dbname": CHECK_DB}
    try:
        # le dump vient d'un autre serveur : ses OWNER/GRANT ne s'appliquent pas
        sql = pathlib.Path(dump).read_text()
        sql = "\n".join(
            line for line in sql.splitlines()
            if not line.startswith(("ALTER TABLE public.import_history OWNER",
                                    "ALTER SEQUENCE", "GRANT ", "REVOKE "))
            and "OWNER TO" not in line
        )
        psql(kwargs, sql)
        psql(kwargs, (MIGRATIONS / "015_create_import_departements.sql").read_text())

        os.environ["ATP2OSM_LOGS_DIR"] = str(logs_dir)
        spec = importlib.util.spec_from_file_location(
            "backfill", MIGRATIONS / "016_backfill_import_departements.py"
        )
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        with psycopg.connect(**kwargs) as conn:
            module.BackfillImportDepartements(conn).migrate()
            report(conn)
            if keep:
                conn.commit()
            else:
                conn.rollback()
    finally:
        if keep:
            print(
                f"\nBase conservée. Pour l'inspecter :\n"
                f"  psql -h {kwargs['host']} -p {kwargs['port']} "
                f"-U {kwargs['user']} -d {CHECK_DB}\n"
                f"Pour la supprimer :\n"
                f"  dropdb -h {kwargs['host']} -p {kwargs['port']} "
                f"-U {kwargs['user']} {CHECK_DB}"
            )
        else:
            with psycopg.connect(**admin) as conn:
                conn.execute(f'DROP DATABASE IF EXISTS "{CHECK_DB}"')


def report(conn):
    def q(sql, *params):
        return conn.execute(sql, params).fetchall()

    total = q("SELECT COUNT(*) FROM import_history")[0][0]
    detailed = q(
        "SELECT COUNT(DISTINCT import_id) FROM import_departements"
    )[0][0]
    lines = q("SELECT COUNT(*) FROM import_departements")[0][0]

    print(f"\n{'=' * 70}\nRAPPORT DE REPRISE\n{'=' * 70}")
    print(f"{total} intégration(s) dans l'historique")
    print(f"{detailed} détaillée(s), soit {lines} ligne(s) import_departements")

    print("\nStatuts (intégration) :")
    for status, n, avec in q(
        """SELECT ih.status, COUNT(*),
                  COUNT(*) FILTER (WHERE EXISTS (
                      SELECT 1 FROM import_departements d WHERE d.import_id = ih.id))
           FROM import_history ih GROUP BY ih.status ORDER BY 2 DESC"""
    ):
        print(f"  {status:<16} {n:>4}  dont {avec:>4} détaillée(s)")

    print("\nStatuts (département) :")
    for status, n in q(
        "SELECT status, COUNT(*) FROM import_departements GROUP BY status ORDER BY 2 DESC"
    ):
        print(f"  {status:<16} {n:>4}")

    # Écart 1 : le statut dérivé des enfants doit correspondre au statut stocké
    # (aux suffixes près, que la migration 018 supprime).
    print("\nÉcarts entre le statut stocké et celui que la reprise déduit :")
    children = collections.defaultdict(list)
    for import_id, status in q(
        "SELECT import_id, status FROM import_departements"
    ):
        children[import_id].append({"status": status})
    mismatches = 0
    for import_id, status in q(
        "SELECT id, status FROM import_history ORDER BY id"
    ):
        if import_id not in children:
            continue
        derived = _determine_import_status(children[import_id])
        if derived != status.split("_")[0]:
            mismatches += 1
            print(f"  #{import_id} : stocké {status}, déduit {derived}")
    print(f"  {mismatches} écart(s)")

    # Écart 2 : les POIs annoncés par l'historique et ceux des départements
    # réussis doivent coïncider.
    print("\nÉcarts sur items_count :")
    rows = q(
        """SELECT ih.id, ih.items_count,
                  COALESCE(SUM(d.items_count) FILTER (WHERE d.status = 'success'), 0)
           FROM import_history ih
           JOIN import_departements d ON d.import_id = ih.id
           GROUP BY ih.id, ih.items_count
           HAVING ih.items_count IS DISTINCT FROM
                  COALESCE(SUM(d.items_count) FILTER (WHERE d.status = 'success'), 0)
           ORDER BY ih.id"""
    )
    for import_id, stored, computed in rows:
        print(f"  #{import_id} : historique {stored}, départements {computed}")
    print(f"  {len(rows)} écart(s)")

    # Ce qui reste sans détail, et pourquoi. Un abandon et une intégration
    # vide n'ont rien à détailler : ce sont les autres qui coûtent.
    print("\nIntégrations sans détail, par raison :")
    for reason, n in q(
        """SELECT CASE
                    WHEN status = 'cancelled'  THEN 'abandon (rien à détailler)'
                    WHEN items_count = 0       THEN 'aucun POI à intégrer'
                    ELSE 'log manquant ou inexploitable'
                  END, COUNT(*)
           FROM import_history ih
           WHERE NOT EXISTS (SELECT 1 FROM import_departements d WHERE d.import_id = ih.id)
           GROUP BY 1 ORDER BY 2 DESC"""
    ):
        print(f"  {reason:<32} {n:>4}")
    print("\n  Détail des pertes réelles :")
    for import_id, status, items, brand, date in q(
        """SELECT id, status, items_count, brand_wikidata, import_date::date
           FROM import_history ih
           WHERE NOT EXISTS (SELECT 1 FROM import_departements d WHERE d.import_id = ih.id)
             AND status <> 'cancelled' AND items_count IS DISTINCT FROM 0
           ORDER BY id"""
    ):
        print(f"    #{import_id:<4} {status:<16} {str(items):>5} POIs  {brand} {date}")
    kept = q(
        """SELECT COUNT(*) FROM import_history
           WHERE comment LIKE '%%Changesets : %%'"""
    )[0][0]
    print(f"  dont {kept} avec leurs changesets conservés dans le commentaire")

    orphans = q(
        """SELECT COUNT(*) FROM import_departements
           WHERE status <> 'success' AND osm_changeset_id IS NULL"""
    )[0][0]
    print(f"\n{orphans} département(s) en échec sans changeset (création échouée)")
    print("=" * 70)


if __name__ == "__main__":
    if len(sys.argv) not in (3, 4):
        sys.exit(__doc__)
    main(sys.argv[1], pathlib.Path(sys.argv[2]).resolve(), "--keep" in sys.argv)
