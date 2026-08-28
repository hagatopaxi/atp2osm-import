"""Geofabrik outage must not fail the pipeline: we keep the data we have."""
from datetime import datetime, timezone

import pytest

from src.pipeline import osm
from src.pipeline.errors import SourceUnavailable


@pytest.fixture(autouse=True)
def _isolate_timestamp_file(tmp_path, monkeypatch):
    monkeypatch.setattr(osm, "GEOFABRIK_TS_PATH", tmp_path / "geofabrik-timestamp.txt")


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


def test_the_answer_is_reused_without_a_second_round_trip(monkeypatch):
    """osm-probe, osm-download and osm-views must not each hit the network."""
    calls = []
    ts = datetime(2026, 8, 27, tzinfo=timezone.utc)
    monkeypatch.setattr(osm, "GEOFABRIK_REGIONS", {"france": {}})
    monkeypatch.setattr(osm, "_geofabrik_timestamp",
                        lambda region: (calls.append(region), ts)[1])
    assert osm._newest_geofabrik_timestamp() == ts
    assert osm._newest_geofabrik_timestamp() == ts
    assert len(calls) == 1


def test_an_outage_is_remembered_too(monkeypatch):
    """Otherwise the next step re-runs the whole retry sequence for nothing."""
    calls = []
    monkeypatch.setattr(osm, "GEOFABRIK_REGIONS", {"france": {}})
    monkeypatch.setattr(osm, "_geofabrik_timestamp",
                        lambda region: (calls.append(region), _boom())[1])
    assert osm._newest_geofabrik_timestamp() is None
    assert osm._newest_geofabrik_timestamp() is None
    assert len(calls) == 1


def test_forget_clears_it(monkeypatch):
    osm.GEOFABRIK_TS_PATH.parent.mkdir(parents=True, exist_ok=True)
    osm.GEOFABRIK_TS_PATH.write_text("2020-01-01T00:00:00+00:00")
    osm.forget_geofabrik_timestamp()
    assert not osm.GEOFABRIK_TS_PATH.exists()
