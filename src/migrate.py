import logging
import pathlib
import re

logger = logging.getLogger(__name__)

MIGRATIONS_DIR = pathlib.Path(__file__).parent.parent / "migrations"


def _ensure_schema_migrations_table(cursor):
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS schema_migrations (
            version     INTEGER PRIMARY KEY,
            filename    TEXT NOT NULL,
            applied_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
    """)


def _get_applied_versions(cursor):
    cursor.execute("SELECT version FROM schema_migrations ORDER BY version;")
    return {row[0] for row in cursor.fetchall()}


def _discover_migrations():
    """Return sorted list of (version, filepath) from the migrations directory."""
    pattern = re.compile(r"^(\d+)_.+\.sql$")
    migrations = []

    if not MIGRATIONS_DIR.is_dir():
        logger.warning(f"Migrations directory not found: {MIGRATIONS_DIR}")
        return migrations

    for path in sorted(MIGRATIONS_DIR.iterdir()):
        match = pattern.match(path.name)
        if match:
            version = int(match.group(1))
            migrations.append((version, path))

    return migrations


def run_migrations(conn):
    """Run all pending SQL migrations. Called at server startup."""
    logger.info("Checking for pending migrations...")

    with conn.cursor() as cursor:
        _ensure_schema_migrations_table(cursor)
        conn.commit()

        applied = _get_applied_versions(cursor)
        migrations = _discover_migrations()
        pending = [(v, p) for v, p in migrations if v not in applied]

        if not pending:
            logger.info("No pending migrations.")
            return

        logger.info(f"{len(pending)} pending migration(s) to apply.")

        for version, path in pending:
            logger.info(f"Applying migration {path.name}...")
            try:
                sql = path.read_text(encoding="utf-8")
                cursor.execute(sql)
                cursor.execute(
                    "INSERT INTO schema_migrations (version, filename) VALUES (%s, %s);",
                    (version, path.name),
                )
                conn.commit()
                logger.info(f"Migration {path.name} applied successfully.")
            except Exception:
                conn.rollback()
                logger.exception(f"Migration {path.name} failed.")
                raise

    logger.info("All migrations applied.")
