import pytest

from src.migrate import Migration, _discover_migrations, _run_python_migration

# la « connexion » est ici une liste, pour voir ce que la migration en fait
ONE_CLASS = """
from src.migrate import Migration


class Bump(Migration):
    def migrate(self):
        self.conn.append("ran")
"""

NO_CLASS = "x = 1\n"

TWO_CLASSES = """
from src.migrate import Migration


class A(Migration):
    def migrate(self):
        pass


class B(Migration):
    def migrate(self):
        pass
"""


def write(tmp_path, name, source):
    path = tmp_path / name
    path.write_text(source)
    return path


def test_runs_the_migration_class_with_the_connection(tmp_path):
    conn = []
    _run_python_migration(write(tmp_path, "100_bump.py", ONE_CLASS), conn)
    assert conn == ["ran"]


def test_refuses_a_file_without_migration_class(tmp_path):
    with pytest.raises(RuntimeError, match="exactly one"):
        _run_python_migration(write(tmp_path, "100_none.py", NO_CLASS), None)


def test_refuses_a_file_with_two_migration_classes(tmp_path):
    with pytest.raises(RuntimeError, match="exactly one"):
        _run_python_migration(write(tmp_path, "100_two.py", TWO_CLASSES), None)


def test_base_class_refuses_to_run_on_its_own():
    with pytest.raises(NotImplementedError):
        Migration(None).migrate()


def test_discovers_both_sql_and_python_migrations_in_order():
    found = _discover_migrations()
    versions = [v for v, _ in found]
    assert versions == sorted(versions)
    suffixes = {p.suffix for _, p in found}
    assert suffixes == {".sql", ".py"}
    # the backfill is a Python migration, and it comes before dropping the
    # column it reads
    by_version = dict(found)
    assert by_version[16].suffix == ".py"
    assert by_version[17].suffix == ".sql"
