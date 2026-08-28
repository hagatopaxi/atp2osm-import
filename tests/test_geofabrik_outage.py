"""Geofabrik outage must not fail the pipeline: we keep the data we have."""
from datetime import datetime, timezone

import pytest

from src.pipeline import osm
from src.pipeline.errors import SourceUnavailable


@pytest.fixture(autouse=True)
def _clear_timestamp_cache():
    osm._newest_geofabrik_timestamp.cache_clear()


def test_returns_none_when_every_region_is_down(monkeypatch):
    monkeypatch.setattr(osm, "GEOFABRIK_REGIONS", {"france": {}, "belgium": {}})
    monkeypatch.setattr(osm, "_geofabrik_timestamp", _boom)
    assert osm._newest_geofabrik_timestamp() is None


def test_uses_the_regions_that_did_answer(monkeypatch):
    ts = datetime(2026, 8, 27, tzinfo=timezone.utc)
    monkeypatch.setattr(osm, "GEOFABRIK_REGIONS", {"france": {}, "belgium": {}})
    monkeypatch.setattr(
        osm, "_geofabrik_timestamp",
        lambda region: _boom(region) if region is osm.GEOFABRIK_REGIONS["france"] else ts,
    )
    assert osm._newest_geofabrik_timestamp() == ts


def test_download_reports_the_source_as_unavailable(monkeypatch):
    """Not a pipeline failure: SourceUnavailable, and the DB is never touched."""
    monkeypatch.setattr(osm, "_newest_geofabrik_timestamp", lambda: None)
    monkeypatch.setattr(osm, "connect", _boom)  # no pending row, no maintenance
    try:
        osm.download_pbf()
    except SourceUnavailable:
        pass
    else:
        raise AssertionError("expected SourceUnavailable")


def _boom(*_args):
    raise RuntimeError("502 Bad Gateway")


def test_refuses_a_partial_import(tmp_path, monkeypatch):
    """A leftover PBF from a failed run must not become the whole planet."""
    present, absent = tmp_path / "belgium.pbf", tmp_path / "france.pbf"
    present.write_bytes(b"x")
    monkeypatch.setattr(osm, "GEOFABRIK_REGIONS", {
        "belgium": {"pbf_path": present}, "france": {"pbf_path": absent},
    })
    try:
        osm.run_osm2pgsql()
    except RuntimeError as exc:
        assert "france.pbf" in str(exc)
    else:
        raise AssertionError("imported a partial region set")
