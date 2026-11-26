"""Offline tests for the shared market-data adapter (injected transport)."""
import importlib.util
import os
import sys
from datetime import date, timedelta
from pathlib import Path

import pytest

_MOD_PATH = "/mnt/shared/hermes/market-data-adapter/market_data_adapter.py"


def load_module():
    spec = importlib.util.spec_from_file_location("market_data_adapter", _MOD_PATH)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


def make_bars(mod, expected_date_iso, count=15, close0=100.0):
    d = date.fromisoformat(expected_date_iso)
    dates = []
    while len(dates) < count:
        if d.weekday() < 5:
            dates.append(d)
        d -= timedelta(days=1)
    dates.reverse()
    close = close0
    bars = []
    for dt in dates:
        close += 1
        bars.append({
            "t": f"{dt.isoformat()}T05:00:00Z", "o": close - 1, "h": close + 0.5,
            "l": close - 1.5, "c": close, "v": 1000, "n": 10, "vw": close - 0.1,
        })
    return bars


@pytest.fixture()
def mod(tmp_path, monkeypatch):
    m = load_module()
    monkeypatch.setenv("MARKET_CACHE_PATH", str(tmp_path / "bars.db"))
    monkeypatch.setenv("ALPACA_API_KEY", "k")   # pragma: allowlist secret
    monkeypatch.setenv("ALPACA_SECRET_KEY", "s")
    yield m


def test_cache_hit_zero_fetch_on_second_call(mod, tmp_path):
    expected = mod.expected_latest_session()
    calls = {"n": 0}

    def fake_fetch(path, params):
        calls["n"] += 1
        return {"bars": {s: make_bars(mod, expected) for s in params["symbols"].split(",")}}

    md = mod.MarketData(cache_path=str(tmp_path / "bars.db"), fetch=fake_fetch)
    first = md.daily_bars("SPY", 10)
    assert calls["n"] == 1
    assert len(first) == 10 and first[-1]["date"] == expected
    second = md.daily_bars("SPY", 10)
    assert calls["n"] == 1  # served entirely from cache
    assert [r["date"] for r in second] == [r["date"] for r in first]


def test_gap_fill_fetches_only_missing_symbol(mod, tmp_path):
    expected = mod.expected_latest_session()
    seen = []

    def fake_fetch(path, params):
        seen.append(params["symbols"])
        return {"bars": {s: make_bars(mod, expected) for s in params["symbols"].split(",")}}

    md = mod.MarketData(cache_path=str(tmp_path / "bars.db"), fetch=fake_fetch)
    md.daily_bars("AAA", 10)
    md.daily_bars("BBB", 10)
    md.daily_bars("AAA", 10)
    assert seen[0] == "AAA" and seen[1] == "BBB" and len(seen) == 2


def test_batch_seeded_from_cache(mod, tmp_path):
    expected = mod.expected_latest_session()
    calls = {"n": 0}

    def fake_fetch(path, params):
        calls["n"] += 1
        return {"bars": {s: make_bars(mod, expected) for s in params["symbols"].split(",")}}

    md = mod.MarketData(cache_path=str(tmp_path / "bars.db"), fetch=fake_fetch)
    md.daily_bars_multi(["AAA", "BBB", "CCC"], 10)
    assert calls["n"] == 1
    out = md.daily_bars_multi(["AAA", "BBB", "CCC"], 10)
    assert calls["n"] == 1
    assert all(len(rows) == 10 and rows[-1]["date"] == expected for rows in out.values())


def test_bars_survive_across_instances_shared_db(mod, tmp_path):
    expected = mod.expected_latest_session()
    calls = {"n": 0}

    def fake_fetch(path, params):
        calls["n"] += 1
        return {"bars": {s: make_bars(mod, expected, close0=calls["n"] * 10) for s in params["symbols"].split(",")}}

    one = mod.MarketData(cache_path=str(tmp_path / "shared.db"), fetch=fake_fetch)
    one.daily_bars("SPY", 10)  # caches 150 synthetic rows (window overshoot)
    two = mod.MarketData(cache_path=str(tmp_path / "shared.db"), fetch=fake_fetch)
    rows = two.daily_bars("SPY", 10)
    assert calls["n"] == 1  # second instance reads the shared db


def test_quotes_not_cached(mod, tmp_path):
    expected = mod.expected_latest_session()
    calls = {"n": 0}

    def fake_fetch(path, params):
        calls["n"] += 1
        if path == "snapshots":
            return {s: {"dailyBar": {"c": 10 + calls["n"], "v": 1}, "prevDailyBar": {"c": 10}}
                    for s in params["symbols"].split(",")}
        return {"bars": {s: make_bars(mod, expected) for s in params["symbols"].split(",")}}

    md = mod.MarketData(cache_path=str(tmp_path / "bars.db"), fetch=fake_fetch)
    q1 = md.quote("SPY")["price"]
    q2 = md.quote("SPY")["price"]
    assert calls["n"] == 2  # quotes always live
    assert q1 != q2 and q2 == 12.0


def test_dash_symbols_map_to_alpaca_convention(mod, tmp_path):
    expected = mod.expected_latest_session()

    def fake_fetch(path, params):
        return {"bars": {s: make_bars(mod, expected) for s in params["symbols"].split(",")}}

    md = mod.MarketData(cache_path=str(tmp_path / "bars.db"), fetch=fake_fetch)
    rows = md.daily_bars("BRK-B", 5)
    assert len(rows) == 5
    q = md.quote("BRK-B")
    assert q["symbol"] == "BRK-B"  # back to FMP convention on the way out


def test_index_symbols_return_empty(mod, tmp_path):
    md = mod.MarketData(cache_path=str(tmp_path / "bars.db"))
    assert md.daily_bars("^GSPC", 5) == []


def test_paid_feed_falls_back_to_iex_on_403(mod, tmp_path):
    expected = mod.expected_latest_session()
    calls = []

    def fake_fetch(path, params):
        calls.append(params.get("feed"))
        if params.get("feed") == "sip":
            instance.last_error = "HTTP 403"
            return None
        return {"bars": {s: make_bars(mod, expected) for s in params["symbols"].split(",")}}

    instance = mod.MarketData(cache_path=str(tmp_path / "bars.db"), feed="sip", fetch=fake_fetch)
    rows = instance.daily_bars("AAPL", 5)
    assert len(rows) == 5
    assert calls == ["sip", "iex"]  # refused once, then degraded and sticky
    assert instance._feed_locked is True
    # Subsequent calls skip the refused feed entirely.
    calls.clear()
    rows2 = instance.daily_bars("AAPL", 5)
    assert len(rows2) == 5
    assert calls == []  # cache hit, nothing fetched
    instance.daily_bars("MSFT", 5)
    assert calls == ["iex"]  # sticky: never retries sip


def test_bar_end_clamped_to_last_completed_session_sip_ok(mod, tmp_path):
    expected = mod.expected_latest_session()
    seen = []

    def fake_fetch(path, params):
        seen.append(params)
        return {"bars": {s: make_bars(mod, params["end"], count=10) for s in params["symbols"].split(",")}}

    md = mod.MarketData(cache_path=str(tmp_path / "bars.db"), fetch=fake_fetch)
    md.daily_bars("PLD", 5)
    assert seen[0]["end"] == expected  # never reaches into the live session
    assert seen[0]["feed"] == "sip"    # full-tape fetch, no 403 fallback needed
