"""Granularity of the statistics charts, derived from the filtered period."""

from datetime import date, timedelta

from src.routes.stats import _period


def _unit(days):
    stop = date.today()
    unit, _start, _end = _period((stop - timedelta(days=days)).isoformat(), stop.isoformat())
    return unit


def test_short_period_is_shown_day_by_day():
    assert _unit(6) == "day"


def test_medium_period_is_shown_week_by_week():
    assert _unit(55) == "week"


def test_long_period_is_shown_month_by_month():
    assert _unit(364) == "month"


def test_without_a_lower_bound_the_series_starts_at_the_first_integration():
    unit, start, end = _period(None, None)
    assert unit == "month"
    assert "MIN(import_date)" in start
    assert date.today().isoformat() in end


def test_an_unparsable_bound_is_ignored():
    assert _period("not-a-date", "neither")[0] == "month"
