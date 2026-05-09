#!/usr/bin/env python3
"""NASDAQ-wide market snapshot, filtered to market cap >= 5,000억 KRW.

The KR equivalent (`snapshot_market.py`) scans every KOSPI/KOSDAQ ticker via
Naver. This script does the same for NASDAQ:

  1. Pull the full NASDAQ-listed symbol directory from NASDAQ Trader (free,
     no auth, ~5,500 entries).
  2. Drop non-common-stock entries (units / rights / warrants / SPACs /
     ETFs / test issues).
  3. For each remaining ticker, fetch:
        - market cap + shares outstanding (yfinance.fast_info)
        - 1-year daily close history + 52w hi/lo (Yahoo chart endpoint)
  4. Filter at MIN_MARKET_CAP_KRW (default 5천억 원, ~$362M at 1,380 KRW/USD).
  5. Compute 1D/1W/1M/3M/6M/1Y returns + 60-day sparkline.
  6. Write data/us-market-snapshot.json.

The frontend (`us-market.html`) loads the JSON and renders a sortable table
similar to `market-scan.html`. Click a row to see chart + headlines + brief.

News headlines are NOT fetched here (would balloon runtime). They are pulled
on-demand by the frontend or by a smaller follow-up cron for top tickers.
"""

import json
import math
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

import requests

# yfinance for shares + market cap (handles Yahoo's crumb auth internally)
import yfinance as yf

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUTPUT_FILE = os.path.join(ROOT, "data", "us-market-snapshot.json")

USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0 Safari/537.36"
)

NASDAQ_LIST_URL = "https://www.nasdaqtrader.com/dynamic/symdir/nasdaqlisted.txt"
CHART_URL = (
    "https://query1.finance.yahoo.com/v8/finance/chart/"
    "{symbol}?range=1y&interval=1d&includePrePost=false"
)

# 5천억 원 ~ $362M USD. Using 1380 KRW/USD as a stable conservative rate.
KRW_PER_USD = 1380
MIN_MARKET_CAP_KRW = 500_000_000_000  # 5천억 원
MIN_MARKET_CAP_USD = MIN_MARKET_CAP_KRW / KRW_PER_USD  # ~$362M

WORKERS = 8  # Yahoo aggressively rate-limits the crumb endpoint; keep modest
RETRY = 1
YF_RETRY = 3  # extra retries for yfinance fast_info under rate limiting
SPARK_LEN = 60

# Symbol-name patterns to skip (non-common-stock listings)
SKIP_NAME_RE = re.compile(
    r"\b("
    r"Warrant|Right|Unit|Preferred|Notes|Bond|Trust|"
    r"ETF|Exchange[- ]Traded|ETN|"
    r"Closed[- ]End|Acquisition Corp"
    r")\b",
    re.IGNORECASE,
)
# Symbol suffixes that mark non-common shares
# Common SPAC/units/rights suffix patterns: ends in W, R, U after >=4 letters
SKIP_SUFFIX_RE = re.compile(r"^[A-Z]{2,4}(W|R|U|WS|RT|UN)$")


def load_nasdaq_universe():
    """Fetch NASDAQ Trader symbol directory and return list of common stocks.

    Returns list of dicts with keys: symbol, name, etf, financial_status.
    Excludes test issues, ETFs, units/rights/warrants, deficient filers.
    """
    r = requests.get(NASDAQ_LIST_URL, timeout=30)
    r.raise_for_status()
    lines = r.text.splitlines()
    # First line is header, last line is "File Creation Time" footer
    header = lines[0].split("|")
    idx = {name: i for i, name in enumerate(header)}

    out = []
    for line in lines[1:]:
        if not line or line.startswith("File Creation Time"):
            continue
        parts = line.split("|")
        if len(parts) < len(header):
            continue
        sym = parts[idx["Symbol"]].strip()
        name = parts[idx["Security Name"]].strip()
        test = parts[idx["Test Issue"]].strip()
        etf = parts[idx["ETF"]].strip()
        fin = parts[idx["Financial Status"]].strip()

        if test == "Y" or etf == "Y":
            continue
        # 'D' = Deficient (not in compliance), 'E' = Delinquent, 'Q' = bankrupt
        if fin in ("D", "E", "Q", "G", "H"):
            continue
        if SKIP_NAME_RE.search(name):
            continue
        # SPACs are noisy and typically tiny — skip aggressively by name
        if "SPAC" in name.upper() or "Acquisition" in name:
            continue
        if SKIP_SUFFIX_RE.match(sym):
            continue
        # Tickers with dots/dashes are usually preferred shares
        if "$" in sym or "." in sym:
            continue

        out.append({"symbol": sym, "name": name})
    return out


def make_session():
    s = requests.Session()
    s.headers.update(
        {
            "User-Agent": USER_AGENT,
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
        }
    )
    return s


def get_chart(session, symbol):
    """Return (closes_oldest_first, timestamps, meta) or None on failure."""
    last = None
    for _ in range(RETRY + 1):
        try:
            r = session.get(CHART_URL.format(symbol=symbol), timeout=20)
            if r.status_code == 200:
                j = r.json()
                res = (j.get("chart") or {}).get("result") or []
                if not res:
                    return None
                res = res[0]
                meta = res.get("meta") or {}
                ts = res.get("timestamp") or []
                quote = ((res.get("indicators") or {}).get("quote") or [{}])[0]
                raw = quote.get("close") or []
                closes, times = [], []
                for t, c in zip(ts, raw):
                    if c is not None and c > 0:
                        closes.append(float(c))
                        times.append(int(t))
                if len(closes) >= 5:
                    return closes, times, meta
                return None
            last = f"HTTP {r.status_code}"
        except Exception as e:
            last = str(e)
        time.sleep(0.4)
    return None


def pct_return(closes, days):
    if len(closes) <= days:
        return None
    cur = closes[-1]
    past = closes[-1 - days]
    if past <= 0:
        return None
    return round((cur / past - 1) * 100, 2)


def fetch_market_cap(symbol):
    """Use yfinance fast_info for market cap + shares. Returns (mc, shares, price).

    Retries on rate-limit errors with exponential backoff. Returns (None, None, None)
    only if every retry fails.
    """
    last_err = None
    for attempt in range(YF_RETRY + 1):
        try:
            t = yf.Ticker(symbol)
            fi = t.fast_info
            mc = float(fi.market_cap) if fi.market_cap else None
            sh = int(fi.shares) if fi.shares else None
            px = float(fi.last_price) if fi.last_price else None
            return mc, sh, px
        except Exception as e:
            last_err = e
            msg = str(e).lower()
            if "rate" in msg or "429" in msg or "too many" in msg:
                # exponential backoff: 2, 4, 8 seconds
                time.sleep(2 ** (attempt + 1))
                continue
            return None, None, None
    return None, None, None


def process_ticker(session, entry):
    """Fetch market cap + history. Return None if below threshold or error."""
    sym = entry["symbol"]
    mc, shares, _px_fast = fetch_market_cap(sym)
    if mc is None or mc < MIN_MARKET_CAP_USD:
        return None  # below threshold or no data — drop

    chart = get_chart(session, sym)
    if not chart:
        return None
    closes, times, meta = chart

    cur = float(meta.get("regularMarketPrice") or closes[-1])
    prev = float(meta.get("chartPreviousClose") or closes[-2])

    spark = closes[-SPARK_LEN:] if len(closes) >= SPARK_LEN else closes[:]

    # Prefer the long name from chart meta (e.g. "Apple Inc.") over the
    # NASDAQ Trader directory name (e.g. "Apple Inc. - Common Stock") or the
    # bare symbol used in backfill calls.
    long_name = meta.get("longName") or meta.get("shortName")
    name = long_name or entry.get("name") or sym

    return {
        "symbol": sym,
        "name": name,
        "exchange": meta.get("fullExchangeName") or meta.get("exchangeName") or "NASDAQ",
        "currency": meta.get("currency") or "USD",
        "price": round(cur, 2),
        "prevClose": round(prev, 2),
        "marketCap": round(mc),
        "marketCapKrw": round(mc * KRW_PER_USD),
        "sharesOutstanding": shares,
        "high52w": meta.get("fiftyTwoWeekHigh"),
        "low52w": meta.get("fiftyTwoWeekLow"),
        "r1d": pct_return(closes, 1),
        "r1w": pct_return(closes, 5),
        "r1m": pct_return(closes, 21),
        "r3m": pct_return(closes, 63),
        "r6m": pct_return(closes, 126),
        "r1y": pct_return(closes, 252),
        "spark": [round(c, 2) for c in spark],
        # last 252 closes for the detail chart, with timestamps
        "history": [
            {"t": t, "c": round(c, 2)} for t, c in zip(times, closes)
        ],
    }


def main():
    t0 = time.time()
    print("Loading NASDAQ symbol directory...", flush=True)
    universe = load_nasdaq_universe()
    print(f"  {len(universe)} candidate common stocks (post-filter)", flush=True)

    session = make_session()

    # Single pass: fetch market cap + chart for each ticker, drop those below threshold.
    print(f"\nFetching market cap + 1y history (workers={WORKERS})...", flush=True)
    print(f"Threshold: 5,000억 KRW = ${MIN_MARKET_CAP_USD:,.0f} USD "
          f"(at {KRW_PER_USD} KRW/USD)\n", flush=True)

    qualified = []
    failures = 0
    completed = 0
    started = time.time()
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futs = {ex.submit(process_ticker, session, e): e["symbol"] for e in universe}
        for fut in as_completed(futs):
            completed += 1
            try:
                row = fut.result()
                if row:
                    qualified.append(row)
            except Exception:
                failures += 1
            if completed % 200 == 0:
                rate = completed / (time.time() - started)
                eta = (len(universe) - completed) / max(rate, 0.1)
                print(
                    f"  {completed}/{len(universe)}  qualified={len(qualified)}  "
                    f"fail={failures}  rate={rate:.1f}/s  eta={eta:.0f}s",
                    flush=True,
                )

    # ----- Backfill: ensure well-known mega-caps are present -----
    # Yahoo's crumb-protected endpoints rate-limit aggressively; some big names
    # routinely fail in the bulk pass. Re-attempt them serially with delays.
    EXPECTED = [
        "AAPL", "MSFT", "NVDA", "GOOGL", "GOOG", "AMZN", "META", "TSLA",
        "AVGO", "COST", "NFLX", "ASML", "AMD", "PEP", "TMUS", "CSCO",
        "ADBE", "INTU", "QCOM", "AMAT", "ARM", "AZN", "LIN", "BKNG",
        "TXN", "PANW", "ISRG", "MU", "LRCX", "KLAC", "ABNB", "MELI",
        "GILD", "MRVL", "PYPL", "SBUX", "MDLZ", "REGN", "VRTX",
    ]
    have = {t["symbol"] for t in qualified}
    missing = [s for s in EXPECTED if s not in have]
    if missing:
        print(f"\nBackfilling {len(missing)} expected mega-caps: {missing[:8]}...",
              flush=True)
        backfill_universe = [{"symbol": s, "name": s} for s in missing]
        for entry in backfill_universe:
            time.sleep(0.4)  # gentle pacing
            try:
                row = process_ticker(session, entry)
                if row:
                    # Use the official name from chart meta if we got one
                    qualified.append(row)
                    print(f"  recovered {row['symbol']:6s} mcap=${row['marketCap']/1e9:.1f}B",
                          flush=True)
            except Exception as e:
                print(f"  {entry['symbol']:6s} backfill failed: {e}", flush=True)

    qualified.sort(key=lambda x: x["marketCap"], reverse=True)
    elapsed = time.time() - t0

    now_utc = datetime.now(timezone.utc).replace(microsecond=0)
    payload = {
        "generatedAtUtc": now_utc.isoformat().replace("+00:00", "Z"),
        "version": 1,
        "minMarketCapKrw": MIN_MARKET_CAP_KRW,
        "minMarketCapUsd": MIN_MARKET_CAP_USD,
        "krwPerUsd": KRW_PER_USD,
        "universeSize": len(universe),
        "qualified": len(qualified),
        "fetchSeconds": round(elapsed, 1),
        "tickers": qualified,
    }

    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, separators=(",", ":"))

    size_mb = os.path.getsize(OUTPUT_FILE) / 1e6
    print(
        f"\nWrote {OUTPUT_FILE} ({size_mb:.2f} MB)\n"
        f"  {len(qualified)} qualified / {len(universe)} candidates "
        f"({100*len(qualified)/max(len(universe),1):.1f}%)\n"
        f"  elapsed {elapsed:.0f}s",
        flush=True,
    )

    # Top 10 preview
    print("\nTop 10 by market cap:")
    for t in qualified[:10]:
        print(f"  {t['symbol']:6s} ${t['price']:>9.2f}  "
              f"mcap=${t['marketCap']/1e9:>7.1f}B  ({t['name'][:40]})")


if __name__ == "__main__":
    main()
