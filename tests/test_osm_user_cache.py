from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import src.config
from src import utils


def _stub_api(monkeypatch, calls):
    """Remplace l'appel réseau et enregistre les ids demandés."""

    class Resp:
        def raise_for_status(self):
            pass

        def json(self):
            return {"users": [{"user": {"id": uid, "display_name": f"u{uid}"}} for uid in calls[-1]]}

    def fake_get(url, **kwargs):
        ids = [int(i) for i in url.split("users=")[1].split(",")]
        calls.append(ids)
        return Resp()

    monkeypatch.setattr(utils.requests, "get", fake_get)
    monkeypatch.setattr(
        src.config,
        "get_settings",
        lambda: SimpleNamespace(api_url="http://osm.test", app_version="test"),
    )


def test_cache_avoids_refetch_then_expires(monkeypatch):
    utils._osm_user_cache.clear()
    calls = []
    _stub_api(monkeypatch, calls)

    assert utils.fetch_osm_users([1, 2]) == {1: "u1", 2: "u2"}
    assert calls == [[1, 2]]

    # Deuxième appel : tout est en cache, aucun appel réseau.
    assert utils.fetch_osm_users([1, 2]) == {1: "u1", 2: "u2"}
    assert calls == [[1, 2]]

    # Seul l'id inconnu part sur le réseau.
    assert utils.fetch_osm_users([1, 3]) == {1: "u1", 3: "u3"}
    assert calls[-1] == [3]

    # Une fois périmé, on refetch.
    utils._osm_user_cache[1] = ("u1", datetime.now(timezone.utc) - timedelta(seconds=1))
    utils.fetch_osm_users([1])
    assert calls[-1] == [1]


def test_api_failure_serves_cached(monkeypatch):
    utils._osm_user_cache.clear()
    calls = []
    _stub_api(monkeypatch, calls)
    utils.fetch_osm_users([1])

    def boom(*a, **kw):
        raise RuntimeError("API down")

    monkeypatch.setattr(utils.requests, "get", boom)
    assert utils.fetch_osm_users([1, 2]) == {1: "u1"}
