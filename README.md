# market-data-adapter

Single-file, stdlib-only market-data adapter shared by multiple trading
research repos on this machine (claude-trading-skills fork, mit-trader-skill,
...). Owns the two things every repo needs and none should duplicate:

1. **Alpaca market-data v2 fetch** — daily bars + latest snapshots,
   stdlib urllib (no `requests`/`alpaca-py`/`yfinance` needed).
2. **Shared SQLite daily-bar cache** — read-through with gap-fill, WAL,
   cross-process; one database for all consumers (`MARKET_CACHE_PATH`).

## Install / use

The adapter is checked out on the persistent shared mount:

    /mnt/shared/hermes/market-data-adapter/market_data_adapter.py

Consumers load it by path (stdlib importlib):

```python
import importlib.util, os
spec = importlib.util.spec_from_file_location(
    "shared_market_data_adapter", os.environ["MARKET_DATA_ADAPTER"])
md = importlib.util.module_from_spec(spec)
spec.loader.exec_module(md)

bars = md.MarketData().daily_bars("SPY", days=250)   # oldest-first dicts, gap-filled
quote = md.MarketData().quote("AAPL")                # snapshots, never cached
```

## Environment contract (all optional)

| Variable | Meaning |
| --- | --- |
| `ALPACA_API_KEY` / `ALPACA_SECRET_KEY` | Alpaca credentials |
| `ALPACA_DATA_FEED` | `iex` (default, free plans) or `sip` (paid, full volume) |
| `MARKET_CACHE_PATH` | SQLite cache file (default `/mnt/shared/hermes/market-data/bars.db`) |
| `MARKET_DATA_ADAPTER` | full path consumers put in their env to find this module |

## Semantics

- `daily_bars(symbol, days=N)` returns exactly the N most-recent completed
  daily bars, oldest-first, columns `date/open/high/low/close/adj_close/volume`.
  `adj_close` == raw close (Alpaca `adjustment=raw`; no dividend adjustment).
- Cache hit requires ≥N rows in window **and** newest bar ≥ expected latest
  completed ET session (bars count after ~17:00 ET; holidays self-heal).
- On misses only the missing symbols are fetched (single batched call) and
  upserted with `INSERT OR REPLACE`; the full fetched window is stored.
- Every cache failure fails open to the network path. Symbols like
  `BRK-B`/`BRK.B` are mapped both ways; index symbols (`^GSPC`) return `[]`.
- Quotes are always live.

## Verified

- 7 offline tests (injected transport): cache-hit zero-fetch, gap-fill,
  batch seeding, cross-instance shared DB, live quotes, symbol mapping —
  `pytest test_market_data_adapter.py`
- Cross-repo live check: SPY pull served from the shared `bars.db`
  with 0 network calls after claude-trading-skills warmed it.
