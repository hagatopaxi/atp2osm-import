from src.pipeline import atp


def test_discard_workdir_keeps_the_parquet(tmp_path, monkeypatch):
    """latest.parquet is what lets import_atp no-op; everything else is derived."""
    monkeypatch.setattr(atp, "ATP_DIR", tmp_path)
    (tmp_path / "output.zip").write_text("zip")
    (tmp_path / "stats.json").write_text("{}")
    (tmp_path / "latest.parquet").write_text("parquet")
    for name in ("geojson", "ndgeojson", "split"):
        (tmp_path / name).mkdir()
        (tmp_path / name / "a.geojson").write_text("{}")

    atp._discard_workdir()

    assert [p.name for p in tmp_path.iterdir()] == ["latest.parquet"]


def test_discard_workdir_on_an_already_clean_directory(tmp_path, monkeypatch):
    monkeypatch.setattr(atp, "ATP_DIR", tmp_path)
    atp._discard_workdir()
    assert list(tmp_path.iterdir()) == []
