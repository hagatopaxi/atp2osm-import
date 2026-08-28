"""A failing source must not take the other branches down with it."""
import pytest

from src.pipeline.errors import PipelineIncomplete, SourceUnavailable
from src.pipeline.runner import run


def _pipeline(ran, osm_download):
    step = lambda name: lambda: ran.append(name)
    return {
        "start": (None, ["osm-download", "atp-download"]),
        "osm-download": (osm_download, ["osm-import"]),
        "osm-import": (step("osm-import"), ["mv-brand"]),
        "atp-download": (step("atp-download"), ["atp-import"]),
        "atp-import": (step("atp-import"), ["mv-brand"]),
        "mv-brand": (step("mv-brand"), []),
    }


def _run(pipeline):
    run(pipeline, set(pipeline))


def test_unavailable_source_lets_its_own_branch_finish():
    ran = []
    def down():
        raise SourceUnavailable("Geofabrik")
    with pytest.raises(PipelineIncomplete):
        _run(_pipeline(ran, down))
    # osm-import still runs: it finds no new PBF and leaves the tables alone.
    assert set(ran) == {"osm-import", "atp-download", "atp-import", "mv-brand"}


def test_real_failure_kills_only_its_descendants():
    ran = []
    def boom():
        raise RuntimeError("half-written tables")
    with pytest.raises(RuntimeError):
        _run(_pipeline(ran, boom))
    assert "osm-import" not in ran   # descendant, never run
    assert "mv-brand" not in ran     # joins the dead branch
    assert "atp-import" in ran       # unrelated branch completed


def test_clean_run_raises_nothing():
    ran = []
    _run(_pipeline(ran, lambda: ran.append("osm-download")))
    assert len(ran) == 5


def test_disk_errors_are_not_mistaken_for_an_outage():
    """A full disk must fail loudly, not be recorded as a skipped source."""
    from src.pipeline.errors import unavailable_if_unreachable
    with pytest.raises(OSError):
        with unavailable_if_unreachable("ATP"):
            raise OSError(28, "No space left on device")
