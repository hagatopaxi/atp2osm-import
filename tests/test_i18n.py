"""Language prefix in the path: routing, fallback redirect and switching URL."""

import pytest
from flask import Flask
from flask_babel import get_locale

from src import i18n

LOCALES = ("fr", "de", "it")
TRANSLATED = ("/", "/brands")


@pytest.fixture
def app():
    app = Flask(__name__)
    i18n.init_app(app, LOCALES, TRANSLATED + ("/when",), "Europe/Paris")

    @app.route("/brands")
    def brands():
        return f"{get_locale()}|{i18n.lang_url('de')}"

    @app.route("/api/export")
    def export():
        return "csv"

    return app


def get(app, path, headers=None, cookies=None, follow=False):
    client = app.test_client()
    for name, value in (cookies or {}).items():
        client.set_cookie(name, value)
    return client.get(path, headers=headers or {}, follow_redirects=follow)


def test_the_prefix_selects_the_language(app):
    assert get(app, "/it/brands").text.startswith("it|")
    assert get(app, "/fr/brands").text.startswith("fr|")


def test_the_prefix_wins_over_the_cookie(app):
    assert get(app, "/it/brands", cookies={"lang": "de"}).text.startswith("it|")


def test_a_missing_language_redirects_to_the_negotiated_one(app):
    assert get(app, "/brands").headers["Location"].endswith("/fr/brands")
    assert get(app, "/brands", cookies={"lang": "de"}).headers[
        "Location"
    ].endswith("/de/brands")
    headers = {"Accept-Language": "it-CH,it;q=0.9"}
    assert get(app, "/brands", headers=headers).headers["Location"].endswith(
        "/it/brands"
    )


def test_an_unknown_language_falls_back_to_the_default(app):
    assert get(app, "/es/brands").headers["Location"].endswith("/fr/brands")


def test_the_redirect_keeps_the_query_string(app):
    location = get(app, "/brands?sort=name&page=2").headers["Location"]
    assert location.endswith("/fr/brands?sort=name&page=2")


def test_language_free_paths_are_left_alone(app):
    assert get(app, "/api/export").status_code == 200


def test_the_language_browsed_is_remembered(app):
    assert "lang=de" in get(app, "/de/brands").headers.get("Set-Cookie", "")
    assert not get(app, "/api/export").headers.get("Set-Cookie")


def test_lang_url_keeps_path_and_query(app):
    _, url = get(app, "/it/brands?sort=name&page=2").text.split("|")
    assert url == "/de/brands?sort=name&page=2"


def test_locale_name_is_written_in_its_own_language(app):
    assert i18n.locale_name("de") == "Deutsch"
    assert i18n.locale_name("it") == "Italiano"


def test_alternate_urls_are_absolute_and_language_free_by_default(app):
    with app.test_request_context("/brands", base_url="http://localhost/it"):
        assert i18n.localized_url("de") == "http://localhost/de/brands"
        assert i18n.default_url() == "http://localhost/brands"
        assert i18n.static_url("img/a.png") == "/static/img/a.png"
        assert i18n.static_url("img/a.png", external=True) == (
            "http://localhost/static/img/a.png"
        )


def test_language_free_strips_the_prefix(app):
    with app.test_request_context("/x", base_url="http://localhost/it"):
        assert i18n.language_free("/it/api/export.csv") == "/api/export.csv"
        assert i18n.language_free("/italy") == "/italy"
    with app.test_request_context("/x"):
        assert i18n.language_free("/api/export.csv") == "/api/export.csv"


def test_dates_follow_the_language_and_the_country_timezone(app):
    import datetime

    from flask_babel import format_datetime

    moment = datetime.datetime(2026, 8, 31, 6, 48, tzinfo=datetime.timezone.utc)

    @app.route("/when")
    def when():
        return format_datetime(moment, "short")

    app.config["BABEL_DEFAULT_TIMEZONE"] = "Europe/Paris"
    assert get(app, "/fr/when").text == "31/08/2026 08:48"
    assert get(app, "/de/when").text == "31.08.26, 08:48"


def test_a_bad_language_or_timezone_refuses_to_start(monkeypatch):
    """Validated at startup, because Babel would 500 on every page instead."""
    from src import config

    monkeypatch.setenv("LOCALES", "fr,zz")
    with pytest.raises(config.ConfigError, match="zz"):
        config.get_locales()

    monkeypatch.setenv("LOCALES", "fr,de")
    assert config.get_locales() == ("fr", "de")

    monkeypatch.setenv("TIMEZONE", "Mars/Olympus")
    with pytest.raises(config.ConfigError, match="Mars/Olympus"):
        config.get_timezone()

    monkeypatch.delenv("TIMEZONE")
    assert config.get_timezone() == "Europe/Paris"
