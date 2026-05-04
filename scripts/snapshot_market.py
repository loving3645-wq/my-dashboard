#!/usr/bin/env python3
"""Daily KOSPI/KOSDAQ market snapshot.

Runs from GitHub Actions cron at 18:00 KST (= 09:00 UTC) on weekdays.
Fetches every ticker in tickers-full.js from Naver Finance directly (no CORS
proxy needed when running server-side) and writes data/market-snapshot.json.

The frontend (market-scan.html) loads this JSON on page open so the table is
populated instantly without any browser-side scanning.
"""

import json
import os
import re
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

import requests

KST = timezone(timedelta(hours=9))

NAVER_HISTORY = (
    "https://api.finance.naver.com/siseJson.naver"
    "?symbol={code}&requestType=1&startTime={start}&endTime={end}&timeframe=day"
)
NAVER_TREND = "https://m.stock.naver.com/api/stock/{code}/trend"

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TICKERS_FILE = os.path.join(ROOT, "tickers-full.js")
TICKERS_ETF_FILE = os.path.join(ROOT, "tickers-etf.js")
OUTPUT_FILE = os.path.join(ROOT, "data", "market-snapshot.json")

USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0 Safari/537.36"
)

HISTORY_WORKERS = 24
FLOW_WORKERS = 24
RETRY_COUNT = 2
SPARK_LEN = 60  # last 60 trading days for sparkline (≈ 3 months)

ROW_RE = re.compile(
    r'\[\s*"?(\d{8})"?\s*,'
    r"\s*(-?\d+(?:\.\d+)?)\s*,"
    r"\s*(-?\d+(?:\.\d+)?)\s*,"
    r"\s*(-?\d+(?:\.\d+)?)\s*,"
    r"\s*(-?\d+(?:\.\d+)?)\s*,"
    r"\s*(-?\d+(?:\.\d+)?)"
)


def load_tickers():
    """Extract ticker codes from both tickers-full.js (stocks/SPACs) and
    tickers-etf.js (ETFs). Codes can be 6-digit numeric or 6-char
    alphanumeric — both forms are valid KRX symbols since the new alpha
    code series was introduced for recent ETF listings."""
    codes = set()
    for path in (TICKERS_FILE, TICKERS_ETF_FILE):
        if not os.path.exists(path):
            continue
        with open(path, "r", encoding="utf-8") as f:
            text = f.read()
        # 6 chars: digits, or digits + uppercase letters
        codes.update(re.findall(r"'([0-9A-Z]{6})'\s*:", text))
    return sorted(codes)


def parse_history(text):
    """Parse Naver siseJson rows: ["YYYYMMDD", o, h, l, c, v, foreign%]."""
    closes, vols, dates = [], [], []
    for m in ROW_RE.finditer(text):
        try:
            close = float(m.group(5))
        except ValueError:
            continue
        if close <= 0:
            continue
        closes.append(close)
        try:
            vols.append(float(m.group(6)))
        except ValueError:
            vols.append(0.0)
        dates.append(m.group(1))
    return closes, vols, dates


def pct_return(closes, lookback):
    if len(closes) < 2:
        return None
    cur = closes[0]
    past = closes[min(lookback, len(closes) - 1)]
    if past <= 0:
        return None
    return round((cur / past - 1) * 100, 3)


def get_with_retry(session, url, referer, retries=RETRY_COUNT):
    last_err = None
    for attempt in range(retries + 1):
        try:
            r = session.get(url, headers={"Referer": referer}, timeout=15)
            if r.status_code == 200 and r.text:
                return r
            last_err = f"HTTP {r.status_code}"
        except Exception as e:
            last_err = str(e)
    raise RuntimeError(last_err or "unknown error")


def fetch_history(session, code):
    end = datetime.utcnow()
    start = end - timedelta(days=400)
    url = NAVER_HISTORY.format(
        code=code, start=start.strftime("%Y%m%d"), end=end.strftime("%Y%m%d")
    )
    r = get_with_retry(session, url, referer="https://finance.naver.com/")
    closes, vols, dates = parse_history(r.text)
    if len(closes) < 5:
        return None

    # Normalize to newest-first
    if dates and dates[0] < dates[-1]:
        closes.reverse()
        vols.reverse()
        dates.reverse()

    avg_vol_20 = sum(vols[:20]) / max(1, min(20, len(vols)))
    spark = list(reversed([round(c) for c in closes[:SPARK_LEN]]))

    return {
        "price": round(closes[0]),
        "r1d": pct_return(closes, 1),
        "r1w": pct_return(closes, 5),
        "r1m": pct_return(closes, 22),
        "r3m": pct_return(closes, 66),
        "r6m": pct_return(closes, 125),
        "r1y": pct_return(closes, 248),
        "volume": round(vols[0]),
        "avgVol20": round(avg_vol_20),
        "spark": spark,
    }


def parse_qty(s):
    if s is None:
        return 0
    try:
        return int(float(str(s).replace("+", "").replace(",", "").strip()))
    except (ValueError, TypeError):
        return 0


def fetch_flow(session, code):
    url = NAVER_TREND.format(code=code)
    r = get_with_retry(session, url, referer="https://m.stock.naver.com/")
    j = r.json()
    if not isinstance(j, list) or not j:
        return None
    rows = [
        {
            "foreign": parse_qty(row.get("foreignerPureBuyQuant")),
            "organ": parse_qty(row.get("organPureBuyQuant")),
            "individual": parse_qty(row.get("individualPureBuyQuant")),
        }
        for row in j[:20]
    ]
    last5 = rows[:5]
    last20 = rows[:20]
    summed = lambda arr, k: sum(x[k] for x in arr)
    return {
        "foreign1d": rows[0]["foreign"] if rows else 0,
        "organ1d": rows[0]["organ"] if rows else 0,
        "individual1d": rows[0]["individual"] if rows else 0,
        "foreign5d": summed(last5, "foreign"),
        "organ5d": summed(last5, "organ"),
        "individual5d": summed(last5, "individual"),
        "foreign20d": summed(last20, "foreign"),
        "organ20d": summed(last20, "organ"),
        "individual20d": summed(last20, "individual"),
    }


def make_session():
    s = requests.Session()
    s.headers.update(
        {
            "User-Agent": USER_AGENT,
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "ko-KR,ko;q=0.9,en;q=0.8",
        }
    )
    return s


def main():
    codes = load_tickers()
    print(f"Loaded {len(codes)} tickers", flush=True)

    session = make_session()
    snapshot = {}

    # ---- Phase A: history ----
    print("Phase A: fetching history (24 workers)...", flush=True)
    hist_errors = 0
    completed = 0
    with ThreadPoolExecutor(max_workers=HISTORY_WORKERS) as ex:
        futures = {ex.submit(fetch_history, session, c): c for c in codes}
        for fut in as_completed(futures):
            code = futures[fut]
            completed += 1
            try:
                row = fut.result()
                if row:
                    snapshot[code] = row
            except Exception as e:
                hist_errors += 1
            if completed % 250 == 0:
                print(
                    f"  history {completed}/{len(codes)} "
                    f"(ok={len(snapshot)}, err={hist_errors})",
                    flush=True,
                )
    print(
        f"Phase A done: {len(snapshot)}/{len(codes)} "
        f"({hist_errors} errors)",
        flush=True,
    )

    # ---- Phase B: investor flow (only for tickers with valid history) ----
    # ETFs/funds don't have investor-flow trend data on Naver, so skip them
    # to avoid ~1k pointless 4xx requests per run. The /trend endpoint is
    # for common stocks (corporations) only.
    etf_codes = set()
    if os.path.exists(TICKERS_ETF_FILE):
        with open(TICKERS_ETF_FILE, "r", encoding="utf-8") as f:
            etf_codes = set(re.findall(r"'([0-9A-Z]{6})'\s*:", f.read()))
    print("Phase B: fetching investor flow (24 workers)...", flush=True)
    flow_codes = [c for c in snapshot.keys() if c not in etf_codes]
    print(f"  (skipping {len(snapshot) - len(flow_codes)} ETFs)", flush=True)
    flow_errors = 0
    completed = 0
    with ThreadPoolExecutor(max_workers=FLOW_WORKERS) as ex:
        futures = {ex.submit(fetch_flow, session, c): c for c in flow_codes}
        for fut in as_completed(futures):
            code = futures[fut]
            completed += 1
            try:
                flow = fut.result()
                if flow:
                    snapshot[code]["flow"] = flow
            except Exception:
                flow_errors += 1
            if completed % 250 == 0:
                print(
                    f"  flow {completed}/{len(flow_codes)} "
                    f"(err={flow_errors})",
                    flush=True,
                )
    print(f"Phase B done ({flow_errors} errors)", flush=True)

    # ---- Write snapshot ----
    now_kst = datetime.now(KST)
    output = {
        "generatedAt": now_kst.isoformat(timespec="seconds"),
        "generatedAtUtc": datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
        "version": 1,
        "tickerCount": len(snapshot),
        "totalTickers": len(codes),
        "histErrors": hist_errors,
        "flowErrors": flow_errors,
        "tickers": snapshot,
    }

    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, separators=(",", ":"))

    size_mb = os.path.getsize(OUTPUT_FILE) / 1e6
    print(
        f"Wrote {OUTPUT_FILE} ({size_mb:.2f} MB, {len(snapshot)} tickers, "
        f"generated at {now_kst.isoformat()})"
    )

    if len(snapshot) < len(codes) * 0.5:
        print(
            f"ERROR: snapshot covers only {len(snapshot)}/{len(codes)} tickers — "
            "treating as failure to avoid clobbering a good snapshot.",
            file=sys.stderr,
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
