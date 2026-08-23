from datetime import datetime, timezone

import pytest

from src.pipeline.atp import select_run


def run(day, **kw):
    return {
        "run_id": f"2026-08-{day:02d}",
        "end_time": f"2026-08-{day:02d}T04:00:00Z",
        "parquet_url": "https://example/latest.parquet",
        "output_url": "https://example/output.zip",
        **kw,
    }


def at(day):
    return datetime(2026, 8, day, 4, 0, tzinfo=timezone.utc)


def test_no_import_yet_takes_newest():
    assert select_run([run(20), run(19)], None)["run_id"] == "2026-08-20"


def test_newer_run_is_taken():
    assert select_run([run(20), run(19)], at(19))["run_id"] == "2026-08-20"


def test_same_run_is_skipped():
    assert select_run([run(20), run(19)], at(20)) is None


def test_older_run_is_skipped():
    """Can happen if ATP republishes an older run — must not go backwards."""
    assert select_run([run(19)], at(20)) is None


def test_run_without_parquet_is_ignored():
    assert select_run([run(20, parquet_url=None), run(19)], at(18))["run_id"] == "2026-08-19"


def test_no_usable_run_raises():
    with pytest.raises(RuntimeError):
        select_run([run(20, parquet_url=None)], None)
