#!/usr/bin/env python3
"""Shared market-data adapter for trading research repos (Claude Trading Skills fork, mit-trader-skill, ...).

Single-file, stdlib-only (urllib + sqlite3). Owns two jobs so repos stop
duplicating them:

1. Alpaca market-data v2 REST fetch (daily bars, latest snapshots).
2. A read-through/gap-fill SQLite cache for daily bars keyed by
   MARKET_CACHE_PATH (shared between every consumer on the machine).

Env contract (all optional):
- ALPACA_API_KEY / ALPACA_SECRET_KEY  - credentials
- ALPACA_DATA_FEED                    - iex (default, free plans) or sip
- MARKET_CACHE_PATH                   - sqlite db, e.g. /mnt/shared/hermes/market-data/bars.db
- DATA_SOURCE                         - consumers honor fmp/auto; adapter = alpaca only

Quick use:
    import importlib.util, os
    spec = importlib.util.spec_from_file_location(
        "market_data_adapter",
        os.environ["MARKET_DATA_ADAPTER"])     # e.g. /mnt/shared/hermes/market-data/adapter/market_data_adapter.py
    md = importlib.util.module_from_spec(spec); spec.loader.exec_module(md)
    bars = md.MarketData().daily_bars("SPY", days=250)     # rows oldest-first dict(date=..., close=...)
"""
from __future__ import annotations

import base64
import json
import os
import sqlite3
import stat
import urllib.parse
import urllib.request
from datetime import date, datetime, timedelta
from typing import Callable, Iterable, Optional
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

ALPACA_DATA_BASE = "https://data.alpaca.markets/v2/stocks"
DEFAULT_CACHE_PATH = "/mnt/shared/hermes/market-data/bars.db"


class AdapterError(RuntimeError):
    pass


def _alpaca_symbol(symbol: str) -> str:
    """FMP-style ticker -> Alpaca ticker (BRK-B -> BRK.B). Index symbols pass through flagging."""
    s = (symbol or "").strip().upper()
    return s.replace("-", ".")


def _et_now():
    try:
        return datetime.now(ZoneInfo("America/New_York"))
    except ZoneInfoNotFoundError:  # pragma: no cover - defensive
        return None


def expected_latest_session() -> str:
    """ISO date of the most recent weeknight that should have a completed bar (ET).

    Daily bars land after the 16:00 ET close, so today only counts once it is
    17:00 ET. Holidays self-heal: the expectation fails the freshness check
    and the symbol is refetched.
    """
    now = _et_now()
    if now is None:
        return ""
    day = now.date()
    if day.weekday() >= 5 or now.hour < 17:
        day = day - timedelta(days=1)
        while day.weekday() >= 5:
            day = day - timedelta(days=1)
    return day.isoformat()


def _redact(text: str) -> str:
    """Best-effort secret stripping before any error text is stored or shown."""
    secret = (os.getenv("ALPACA_SECRET_KEY") or "").strip()
    if secret and secret in text:
        text = text.replace(secret, "***")
    return text


class MarketData:
    """Alpaca daily bars + quotes with a shared SQLite cache for bars."""

    def __init__(
        self,
        cache_path: Optional[str] = None,
        feed: Optional[str] = None,
        fetch: Optional[Callable[[str, dict], Optional[dict]]] = None,
        api_key: Optional[str] = None,
        api_secret: Optional[str] = None,
    ):
        self.cache_path = (cache_path or os.getenv("MARKET_CACHE_PATH") or "").strip()
        self.feed = (feed or os.getenv("ALPACA_DATA_FEED") or "sip").strip().lower()
        self.api_key = (api_key or os.getenv("ALPACA_API_KEY") or "").strip()
        self.api_secret = (api_secret or os.getenv("ALPACA_SECRET_KEY") or "").strip()
        self._fetch = fetch  # injectable transport (tests); default = _http_get
        self._feed_locked = False  # True once the paid feed was refused (403)
        self.api_calls = 0
        self.last_error: Optional[str] = None

    # -- transport -----------------------------------------------------------
    def _http_get(self, path: str, params: dict) -> Optional[dict]:
        if not self.api_key or not self.api_secret:
            self.last_error = "ALPACA_API_KEY / ALPACA_SECRET_KEY not set"
            return None
        token = base64.b64encode(f"{self.api_key}:{self.api_secret}".encode()).decode()
        url = f"{ALPACA_DATA_BASE}/{path}?{urllib.parse.urlencode(params)}"
        request = urllib.request.Request(url, headers={"Authorization": f"Basic {token}"})
        self.api_calls += 1
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                if response.status != 200:
                    self.last_error = f"HTTP {response.status}"
                    return None
                return json.loads(response.read().decode())
        except Exception as exc:
            self.last_error = _redact(f"{type(exc).__name__}: {exc}")
            return None

    def _get(self, path: str, params: dict) -> Optional[dict]:
        self.last_error = None
        transport = self._fetch or self._http_get
        if self._feed_locked and params.get("feed") and params["feed"] != "iex":
            params = {**params, "feed": "iex"}
        data = transport(path, params)
        if (
            data is None
            and "403" in (self.last_error or "")
            and self.feed != "iex"
            and not self._feed_locked
        ):
            # Paid feed not entitled for this key: transparently degrade to
            # IEX and stick to it for the process lifetime so all fetched
            # rows keep one consistent tape.
            self._feed_locked = True
            self.last_error = f"feed '{self.feed}' not entitled (403); falling back to 'iex'"
            params = {**params, "feed": "iex"}
            data = transport(path, params)
        return data

    # -- cache ----------------------------------------------------------------
    def _connect(self):
        if not self.cache_path:
            return None
        try:
            parent = os.path.dirname(self.cache_path)
            if parent:
                os.makedirs(parent, exist_ok=True)
            conn = sqlite3.connect(self.cache_path, timeout=5)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA busy_timeout=5000")
            conn.execute(
                "CREATE TABLE IF NOT EXISTS daily_bars ("
                "symbol TEXT NOT NULL, date TEXT NOT NULL,"
                "open REAL, high REAL, low REAL, close REAL, adj_close REAL,"
                "volume REAL, source TEXT NOT NULL DEFAULT 'alpaca',"
                "fetched_at TEXT NOT NULL, PRIMARY KEY (symbol, date))"
            )
            conn.commit()
            return conn
        except Exception as exc:
            self.last_error = _redact(f"cache disabled: {type(exc).__name__}: {exc}")
            return None

    def _cache_hit(self, conn, symbol: str, start: str, end: str, days: int):
        expected = expected_latest_session()
        if not expected:
            return None
        rows = conn.execute(
            "SELECT date, open, high, low, close, adj_close, volume FROM daily_bars"
            " WHERE symbol = ? AND date >= ? AND date <= ?"
            " ORDER BY date DESC LIMIT ?",
            (symbol, start, end, int(days)),
        ).fetchall()
        if len(rows) < int(days) or rows[0][0] < expected:
            return None
        return [
            {
                "date": rec[0], "open": rec[1], "high": rec[2], "low": rec[3],
                "close": rec[4], "adj_close": rec[5], "volume": rec[6],
            }
            for rec in reversed(rows)  # newest-first select -> oldest-first contract
        ]

    def _cache_store(self, conn, symbol: str, bars: Iterable[dict]) -> None:
        stamp = datetime.now(tz=ZoneInfo("UTC")).isoformat(timespec="seconds")
        payload = [
            (symbol, b["date"], b.get("open"), b.get("high"), b.get("low"),
             b.get("close"), b.get("close"), b.get("volume"), "alpaca", stamp)
            for b in bars
            if b.get("date")
        ]
        if payload:
            conn.executemany(
                "INSERT OR REPLACE INTO daily_bars"
                " (symbol, date, open, high, low, close, adj_close, volume, source, fetched_at)"
                " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                payload,
            )
            conn.commit()

    # -- bars ------------------------------------------------------------------
    @staticmethod
    def _bar_to_row(bar: dict) -> dict:
        return {
            "date": (bar.get("t") or "")[:10],
            "open": bar.get("o"),
            "high": bar.get("h"),
            "low": bar.get("l"),
            "close": bar.get("c"),
            "adj_close": bar.get("c"),  # raw bars: no dividend adjustment
            "volume": bar.get("v"),
        }

    def daily_bars(self, symbol: str, days: int = 250) -> list[dict]:
        """Most-recent `days` completed daily bars, oldest-first. [] on failure."""
        rows = self.daily_bars_multi([symbol], days)
        return rows.get(_alpaca_symbol(symbol), [])

    def daily_bars_multi(self, symbols: Iterable[str], days: int = 250) -> dict[str, list[dict]]:
        """Same as daily_bars for several symbols in one call (one gap-fill fetch)."""
        wanted, result = {}, {}
        for s in symbols:
            alp = _alpaca_symbol(s)
            if alp and not alp.startswith("^") and alp not in wanted:
                wanted[alp] = (int(days) if days and int(days) > 0 else 250)
            result[alp or (s or "").strip().upper()] = None
        if not wanted:
            self.last_error = "no fetchable symbols requested"
            return {}
        today = _et_now()
        end_date = today.date().isoformat() if today else date.today().isoformat()
        start_date = (
            today.date() - timedelta(days=max(wanted.values()) * 2 + 5)
        ).isoformat() if today else (date.today() - timedelta(days=505)).isoformat()

        conn = self._connect()
        cached = {}
        if conn is not None:
            try:
                for sym, n in wanted.items():
                    hit = self._cache_hit(conn, sym, start_date, end_date, n)
                    if hit:
                        cached[sym] = hit
            except Exception as exc:
                self.last_error = _redact(f"cache read failed: {type(exc).__name__}: {exc}")
        gaps = [s for s in wanted if s not in cached]
        fetched_rows: dict[str, list[dict]] = {}
        if gaps:
            page_token = None
            for _ in range(20):
                params = {
                    "symbols": ",".join(gaps),
                    "timeframe": "1Day",
                    "start": start_date,
                    "end": end_date,
                    "limit": "10000",
                    "adjustment": "raw",
                    "feed": self.feed,
                }
                if page_token:
                    params["page_token"] = page_token
                data = self._get("bars", params)
                if not data:
                    break
                for sym, bars in (data.get("bars") or {}).items():
                    seen = fetched_rows.setdefault(sym, {})
                    for bar in bars or []:
                        row = self._bar_to_row(bar)
                        if row["date"]:
                            seen[row["date"]] = row
                page_token = data.get("next_page_token")
                if not page_token:
                    break
            # Completed sessions only: an in-progress intraday bar for today
            # (before ~17:00 ET) must never be cached or served as final.
            completed_until = expected_latest_session()
            for sym in list(fetched_rows):
                for dte in [d for d in fetched_rows[sym] if d > completed_until]:
                    del fetched_rows[sym][dte]
            if conn is not None and fetched_rows:
                try:
                    for sym, rows in fetched_rows.items():
                        self._cache_store(conn, sym, [rows[d] for d in sorted(rows)])
                except Exception as exc:
                    self.last_error = _redact(f"cache write failed: {type(exc).__name__}: {exc}")
        for sym, n in wanted.items():
            if sym in cached:
                rows = cached[sym][:n]
            elif fetched_rows.get(sym):
                rows = [fetched_rows[sym][d] for d in sorted(fetched_rows[sym])][-n:]
            elif conn is not None:
                try:
                    recent = self._cache_hit(conn, sym, start_date, end_date, 1)
                except Exception:
                    recent = None
                rows = [recent[0]] if recent and recent[0].get("date") else None
            else:
                rows = None
            result[sym] = rows if rows else []
        if conn is not None:
            conn.close()
        return result

    # -- quotes -----------------------------------------------------------------
    def quote(self, symbol: str) -> dict:
        quotes = self.quotes([symbol])
        return quotes.get(_alpaca_symbol(symbol), {})

    def quotes(self, symbols: Iterable[str]) -> dict[str, dict]:
        """Latest snapshot per symbol (never cached). FMP-ish fields included."""
        alp_syms = []
        out = {}
        for s in symbols:
            alp = _alpaca_symbol(s)
            if alp and not alp.startswith("^") and alp not in alp_syms:
                alp_syms.append(alp)
            out[alp] = {}
        if not alp_syms:
            return {}
        data = self._get("snapshots", {"symbols": ",".join(alp_syms), "feed": self.feed})
        if not data:
            return out
        for sym in alp_syms:
            snap = data.get(sym) or {}
            daily = snap.get("dailyBar") or {}
            prev = snap.get("prevDailyBar") or {}
            trade = snap.get("latestTrade") or {}
            price = trade.get("p") or daily.get("c")
            prev_close = prev.get("c")
            q = {
                "symbol": sym.replace(".", "-"),
                "price": price,
                "open": daily.get("o"),
                "dayHigh": daily.get("h"),
                "dayLow": daily.get("l"),
                "previousClose": prev_close,
                "volume": daily.get("v"),
                "timestamp": trade.get("t") or daily.get("t"),
            }
            if price is not None and prev_close:
                change = price - prev_close
                q["change"] = change
                q["changesPercentage"] = change / prev_close * 100.0
            out[sym] = q
        return out


def load_shared_adapter() -> Optional["MarketData"]:
    """Import the shared adapter module from MARKET_DATA_ADAPTER (consumer helper)."""
    path = (os.getenv("MARKET_DATA_ADAPTER") or "").strip()
    if not path:
        return None
    try:
        import importlib.util

        spec = importlib.util.spec_from_file_location("shared_market_data_adapter", path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module.MarketData()
    except Exception:
        return None


if __name__ == "__main__":  # minimal smoke: python3 market_data_adapter.py SPY 5
    import sys

    sym = sys.argv[1] if len(sys.argv) > 1 else "SPY"
    n = int(sys.argv[2]) if len(sys.argv) > 2 else 5
    md = MarketData()
    rows = md.daily_bars(sym, n)
    print(json.dumps({"symbol": sym, "calls": md.api_calls, "rows": rows[-3:]}, indent=1))
