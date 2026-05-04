#!/usr/bin/env python3
"""Build IPO history dataset for the Post-IPO sheet (2016~present).

Pulls the new-listing master from KRX's public dataset endpoint
(data.krx.co.kr/comm/bldattendant/getJsonData.cmd, bld=MDCSTAT08001),
then enriches each listing with +1W / +1M / +3M / +6M / +1Y price
returns from Naver siseJson.

Designed to run from GitHub Actions runner — KRX/Naver are reachable
directly from there, no CORS proxy needed.

Output: data/ipo-history.json
"""

import json
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

import requests

KST = timezone(timedelta(hours=9))

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUTPUT_FILE = os.path.join(ROOT, "data", "ipo-history.json")

USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
)

KRX_API = "https://data.krx.co.kr/comm/bldattendant/getJsonData.cmd"
KRX_REFERER = "https://data.krx.co.kr/"

# Naver siseJson — same endpoint we use elsewhere
NAVER_HISTORY = (
    "https://api.finance.naver.com/siseJson.naver"
    "?symbol={code}&requestType=1&startTime={start}&endTime={end}&timeframe=day"
)

# Concurrency for Naver per-ticker fetches
PRICE_WORKERS = 16

START_DATE = "20160101"  # 2016-01-01

ROW_RE = re.compile(
    r'\[\s*"?(\d{8})"?\s*,'
    r"\s*(-?\d+(?:\.\d+)?)\s*,"
    r"\s*(-?\d+(?:\.\d+)?)\s*,"
    r"\s*(-?\d+(?:\.\d+)?)\s*,"
    r"\s*(-?\d+(?:\.\d+)?)\s*,"
    r"\s*(-?\d+(?:\.\d+)?)"
)


def krx_session():
    s = requests.Session()
    s.headers.update({
        "User-Agent": USER_AGENT,
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Language": "ko-KR,ko;q=0.9",
        "Referer": KRX_REFERER,
        "X-Requested-With": "XMLHttpRequest",
    })
    return s


def fetch_krx_new_listings(session, start_yyyymmdd, end_yyyymmdd):
    """Pull KRX 'new listing' statistics (MDCSTAT08001) — every IPO/listing
    transition in [start, end] with offering price, count, etc."""
    payload = {
        "bld": "dbms/MDC/STAT/issue/MDCSTAT08001",
        "strtDd": start_yyyymmdd,
        "endDd": end_yyyymmdd,
        "share": "1",
        "csvxls_isNo": "false",
    }
    r = session.post(KRX_API, data=payload, timeout=30)
    r.raise_for_status()
    j = r.json()
    rows = j.get("OutBlock_1") or j.get("output") or []
    out = []
    for r in rows:
        # KRX returns Korean keys; normalize
        code = (r.get("ISU_SRT_CD") or r.get("isu_srt_cd") or "").strip()
        name = (r.get("ISU_NM") or r.get("ISU_ABBRV") or r.get("isu_nm") or "").strip()
        list_dd = (r.get("LIST_DD") or r.get("list_dd") or "").replace("/", "").replace("-", "")
        mkt = (r.get("MKT_TP_NM") or r.get("MKT_NM") or r.get("mkt_tp_nm") or "").strip()
        ipo_price_raw = (r.get("ISU_PRC") or r.get("ISSUE_PRICE") or r.get("isu_prc") or "").strip()
        listed_shares_raw = (r.get("LIST_SHRS") or r.get("LIST_QTY") or "").replace(",", "")
        # Skip non-equity events (REIT mergers, splits, transfers) — keep only fresh IPOs.
        # KRX issue type 'IPO' marker varies by build; if missing we keep the row and let
        # the price-fetch fail filter out non-tradeables.
        if not code or len(code) != 6:
            continue
        if not list_dd or len(list_dd) != 8:
            continue
        try:
            ipo_price = int(re.sub(r"[^\d]", "", ipo_price_raw)) if ipo_price_raw else None
        except ValueError:
            ipo_price = None
        try:
            listed_shares = int(listed_shares_raw) if listed_shares_raw else None
        except ValueError:
            listed_shares = None
        out.append({
            "ticker": code,
            "name": name,
            "listingDate": f"{list_dd[:4]}-{list_dd[4:6]}-{list_dd[6:8]}",
            "market": mkt,
            "ipoPrice": ipo_price,
            "listedShares": listed_shares,
            "marketCapAtListing": (ipo_price * listed_shares) if (ipo_price and listed_shares) else None,
        })
    return out


def naver_session():
    s = requests.Session()
    s.headers.update({
        "User-Agent": USER_AGENT,
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "ko-KR,ko;q=0.9",
        "Referer": "https://finance.naver.com/",
    })
    return s


def fetch_history(session, code, start_yyyymmdd, end_yyyymmdd):
    """Fetch daily OHLCV for [start, end]. Returns list of (date_str, close)."""
    url = NAVER_HISTORY.format(code=code, start=start_yyyymmdd, end=end_yyyymmdd)
    r = session.get(url, timeout=15)
    r.raise_for_status()
    series = []
    for m in ROW_RE.finditer(r.text):
        try:
            close = float(m.group(5))
        except ValueError:
            continue
        if close <= 0:
            continue
        series.append((m.group(1), close))
    series.sort(key=lambda x: x[0])
    return series


def closest_close(series, target_yyyymmdd):
    """Closest trading-day close on/after target. None if none in series."""
    for d, c in series:
        if d >= target_yyyymmdd:
            return c
    return None


def add_days(yyyymmdd, days):
    d = datetime.strptime(yyyymmdd, "%Y%m%d") + timedelta(days=days)
    return d.strftime("%Y%m%d")


def compute_returns(session, ipo):
    """Fetch ~1Y of post-listing prices and compute window returns vs IPO price."""
    listing_dd = ipo["listingDate"].replace("-", "")
    end_dd = add_days(listing_dd, 400)  # ~1Y + buffer
    try:
        series = fetch_history(session, ipo["ticker"], listing_dd, end_dd)
    except Exception:
        return ipo
    if not series:
        return ipo

    first_close = series[0][1]  # first trading-day close
    ipo["firstDayClose"] = round(first_close)
    ipo_p = ipo.get("ipoPrice")
    if ipo_p:
        ipo["firstDayReturn"] = round((first_close / ipo_p - 1) * 100, 2)

    base = ipo_p or first_close
    for label, days in [("r1w", 7), ("r1m", 30), ("r3m", 90), ("r6m", 180), ("r1y", 365)]:
        target = add_days(listing_dd, days)
        c = closest_close(series, target)
        if c is None:
            continue
        ipo[label] = round((c / base - 1) * 100, 2)
        ipo[label + "Close"] = round(c)

    # Drawdown / peak in first year
    closes_only = [c for _, c in series]
    if closes_only:
        peak = max(closes_only)
        trough = min(closes_only)
        ipo["peak1y"] = round((peak / base - 1) * 100, 2)
        ipo["trough1y"] = round((trough / base - 1) * 100, 2)

    return ipo


def main():
    today = datetime.now(KST).strftime("%Y%m%d")
    print(f"Fetching KRX new-listing master {START_DATE} → {today}…", flush=True)
    krx = krx_session()
    listings = fetch_krx_new_listings(krx, START_DATE, today)
    # Dedupe by (ticker, listingDate) — KRX sometimes lists the same event twice
    seen = set()
    deduped = []
    for r in listings:
        key = (r["ticker"], r["listingDate"])
        if key in seen:
            continue
        seen.add(key)
        deduped.append(r)
    listings = deduped
    print(f"  {len(listings)} listings collected", flush=True)
    if len(listings) < 50:
        print("ERROR: KRX returned suspiciously few rows — aborting", file=sys.stderr)
        sys.exit(1)

    # Sort newest-first so the JSON is browser-friendly
    listings.sort(key=lambda r: r["listingDate"], reverse=True)

    # Enrich with Naver price returns (parallel)
    print(f"Enriching {len(listings)} tickers with Naver price returns…", flush=True)
    nv = naver_session()
    completed = [0]
    errors = [0]

    def enrich(ipo):
        try:
            return compute_returns(nv, ipo)
        except Exception:
            errors[0] += 1
            return ipo
        finally:
            completed[0] += 1
            if completed[0] % 100 == 0:
                print(f"  enriched {completed[0]}/{len(listings)} (err {errors[0]})", flush=True)

    with ThreadPoolExecutor(max_workers=PRICE_WORKERS) as ex:
        listings = list(ex.map(enrich, listings))

    # Output
    output = {
        "generatedAt": datetime.now(KST).isoformat(timespec="seconds"),
        "windowFrom": START_DATE,
        "count": len(listings),
        "listings": listings,
    }
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, separators=(",", ":"))
    size_kb = os.path.getsize(OUTPUT_FILE) / 1024
    print(f"Wrote {OUTPUT_FILE} ({size_kb:.1f} KB, {len(listings)} listings)")


if __name__ == "__main__":
    main()
