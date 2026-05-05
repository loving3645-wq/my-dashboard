#!/usr/bin/env python3
"""Build IPO history dataset for the Post-IPO sheet (2016~present).

KRX bot-blocks GitHub Actions runners (returns HTML error page instead of JSON),
so this implementation pulls the new-listing master from 38커뮤니케이션
(http://www.38.co.kr/html/fund/index.htm?o=nw) — same site we already use for
the IPO calendar. Then it enriches each listing with the ticker (from the detail
page) and ±1Y price returns from Naver siseJson.

Output: data/ipo-history.json
"""

import json
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import requests

KST = timezone(timedelta(hours=9))

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUTPUT_FILE = os.path.join(ROOT, "data", "ipo-history.json")

USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
)

BASE_38 = "http://www.38.co.kr"
LIST_URL = f"{BASE_38}/html/fund/index.htm"
DETAIL_URL = f"{BASE_38}/html/fund/"
ENC = "euc-kr"

NAVER_HISTORY = (
    "https://api.finance.naver.com/siseJson.naver"
    "?symbol={code}&requestType=1&startTime={start}&endTime={end}&timeframe=day"
)

START_DATE = "2016-01-01"
DETAIL_WORKERS = 12
PRICE_WORKERS = 16
MAX_PAGES = 80  # 70~75 pages cover 2016 → present

DATE_RE = re.compile(r"(\d{4})/(\d{2})/(\d{2})")
ROW_RE = re.compile(
    r'\[\s*"?(\d{8})"?\s*,'
    r"\s*(-?\d+(?:\.\d+)?)\s*,"
    r"\s*(-?\d+(?:\.\d+)?)\s*,"
    r"\s*(-?\d+(?:\.\d+)?)\s*,"
    r"\s*(-?\d+(?:\.\d+)?)\s*,"
    r"\s*(-?\d+(?:\.\d+)?)"
)


def make_session():
    s = requests.Session()
    s.headers.update({"User-Agent": USER_AGENT})
    return s


def strip_tags(s):
    s = re.sub(r"<br\s*/?>", " ", s, flags=re.I)
    s = re.sub(r"<[^>]+>", "", s)
    s = re.sub(r"&nbsp;", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def to_int(s):
    s = (s or "").replace(",", "").strip()
    return int(s) if s.isdigit() else None


def to_float_pct(s):
    """'12.34%' → 12.34, '-' → None."""
    if not s:
        return None
    m = re.match(r"(-?\d+(?:\.\d+)?)\s*%?", s.strip())
    return float(m.group(1)) if m else None


def parse_listing_page(html):
    """Extract IPO rows from a 38.co.kr `?o=nw&page=N` listing page.

    Observed columns:
      0=name, 1=listingDate(YYYY/MM/DD), 2=시초가, 3=시초가등락률,
      4=공모가, 5=시초가 vs 공모가 등락률(firstDayReturn), 6=현재가,
      7=현재 등락률, 8=parPrice 또는 별도 가격
    Anchor in cell 0 has detail page no=NNNN.
    """
    out = []
    for tr_html in re.findall(r"<tr[^>]*>(.*?)</tr>", html, re.S | re.I):
        if "href=" not in tr_html:
            continue
        cells_raw = re.findall(r"<td[^>]*>(.*?)</td>", tr_html, re.S | re.I)
        if len(cells_raw) < 6:
            continue
        cells = [strip_tags(c) for c in cells_raw]
        name = cells[0]
        if not name or len(name) > 50:
            continue
        m = DATE_RE.match(cells[1])
        if not m:
            continue
        listing_date = f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
        href_m = re.search(r"href=[\"']([^\"']+)[\"']", cells_raw[0])
        no_m = re.search(r"no=(\d+)", href_m.group(1)) if href_m else None
        if not no_m:
            continue
        out.append({
            "name": name,
            "listingDate": listing_date,
            "openPrice": to_int(cells[2]),
            "ipoPrice": to_int(cells[4]),
            "firstDayReturn": to_float_pct(cells[5]),
            "detailNo": no_m.group(1),
        })
    return out


def fetch_all_listings(session, start_date):
    """Paginate ?o=nw until we hit a page older than start_date or run out."""
    all_listings = []
    seen = set()
    for page in range(1, MAX_PAGES + 1):
        try:
            r = session.get(LIST_URL, params={"o": "nw", "page": page}, timeout=20)
            r.encoding = ENC
            r.raise_for_status()
        except Exception as e:
            print(f"  page {page} fetch failed: {e}", file=sys.stderr)
            break
        rows = parse_listing_page(r.text)
        if not rows:
            print(f"  page {page} empty — stopping", flush=True)
            break
        new_count = 0
        for row in rows:
            key = (row["name"], row["listingDate"])
            if key in seen:
                continue
            seen.add(key)
            all_listings.append(row)
            new_count += 1
        oldest = min(row["listingDate"] for row in rows)
        print(f"  page {page}: +{new_count} (oldest {oldest})", flush=True)
        if oldest < start_date:
            break
    return all_listings


TICKER_RE = re.compile(r"종목코드[^0-9]{0,40}(\d{6})")
MARKET_RE = re.compile(r"(코스피|코스닥|KOSPI|KOSDAQ)")


def fetch_detail(session, detail_no):
    """Detail page → (ticker, market). Returns (None, None) on failure."""
    try:
        r = session.get(DETAIL_URL, params={"o": "v", "no": detail_no}, timeout=15)
        r.encoding = ENC
        if r.status_code != 200:
            return (None, None)
        text = r.text
    except Exception:
        return (None, None)
    t = TICKER_RE.search(text)
    m = MARKET_RE.search(text)
    market = None
    if m:
        market = "KOSDAQ" if m.group(1) in ("코스닥", "KOSDAQ") else "KOSPI"
    return (t.group(1) if t else None, market)


def fetch_history(session, code, start_yyyymmdd, end_yyyymmdd):
    """Fetch daily OHLCV for [start, end] from Naver. Returns sorted list of (date_str, close)."""
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
    for d, c in series:
        if d >= target_yyyymmdd:
            return c
    return None


def add_days(yyyymmdd, days):
    d = datetime.strptime(yyyymmdd, "%Y%m%d") + timedelta(days=days)
    return d.strftime("%Y%m%d")


def compute_returns(session, ipo):
    """Fetch ~1Y of post-listing prices, compute window returns vs IPO price."""
    if not ipo.get("ticker"):
        return ipo
    listing_dd = ipo["listingDate"].replace("-", "")
    end_dd = add_days(listing_dd, 400)
    try:
        series = fetch_history(session, ipo["ticker"], listing_dd, end_dd)
    except Exception:
        return ipo
    if not series:
        return ipo

    first_close = series[0][1]
    ipo["firstDayClose"] = round(first_close)
    ipo_p = ipo.get("ipoPrice")
    if ipo_p and ipo.get("firstDayReturn") is None:
        # 38.co.kr already gives firstDayReturn but recompute for consistency
        ipo["firstDayReturn"] = round((first_close / ipo_p - 1) * 100, 2)
    base = ipo_p or first_close
    for label, days in [("r1w", 7), ("r1m", 30), ("r3m", 90), ("r6m", 180), ("r1y", 365)]:
        target = add_days(listing_dd, days)
        c = closest_close(series, target)
        if c is None:
            continue
        ipo[label] = round((c / base - 1) * 100, 2)
        ipo[label + "Close"] = round(c)

    closes = [c for _, c in series]
    if closes:
        ipo["peak1y"] = round((max(closes) / base - 1) * 100, 2)
        ipo["trough1y"] = round((min(closes) / base - 1) * 100, 2)
    return ipo


def main():
    print(f"Fetching 38.co.kr listings since {START_DATE}...", flush=True)
    sess = make_session()
    listings = fetch_all_listings(sess, START_DATE)
    today_str = datetime.now(KST).strftime("%Y-%m-%d")
    # 38.co.kr ?o=nw에는 미상장(상장예정) 종목도 같이 나옴 — listingDate가 미래면
    # 거래 데이터가 없거나 placeholder 값(0%)이 들어가 있어 history에 부적합.
    # 오늘 이전(또는 오늘) 상장만 keep.
    before = len(listings)
    listings = [r for r in listings if START_DATE <= r["listingDate"] <= today_str]
    print(f"  filtered to listed-on-or-before-today: {len(listings)} (dropped {before - len(listings)} future entries)", flush=True)
    print(f"  collected {len(listings)} listings", flush=True)
    if len(listings) < 50:
        print("ERROR: 38.co.kr returned suspiciously few rows — aborting", file=sys.stderr)
        sys.exit(1)

    # Sort newest-first
    listings.sort(key=lambda r: r["listingDate"], reverse=True)

    # Enrich with ticker + market from detail page
    print(f"Enriching {len(listings)} with ticker/market from detail pages...", flush=True)
    completed = [0]

    def enrich_detail(ipo):
        ticker, market = fetch_detail(sess, ipo["detailNo"])
        if ticker:
            ipo["ticker"] = ticker
        if market:
            ipo["market"] = market
        completed[0] += 1
        if completed[0] % 100 == 0:
            print(f"  detail {completed[0]}/{len(listings)}", flush=True)
        return ipo

    with ThreadPoolExecutor(max_workers=DETAIL_WORKERS) as ex:
        listings = list(ex.map(enrich_detail, listings))
    with_ticker = sum(1 for r in listings if r.get("ticker"))
    print(f"  ticker resolved: {with_ticker}/{len(listings)}", flush=True)

    # Enrich with Naver returns
    print(f"Enriching with Naver price returns...", flush=True)
    naver = make_session()
    naver.headers.update({"Referer": "https://finance.naver.com/"})
    completed[0] = 0

    def enrich_prices(ipo):
        try:
            return compute_returns(naver, ipo)
        except Exception:
            return ipo
        finally:
            completed[0] += 1
            if completed[0] % 100 == 0:
                print(f"  prices {completed[0]}/{len(listings)}", flush=True)

    with ThreadPoolExecutor(max_workers=PRICE_WORKERS) as ex:
        listings = list(ex.map(enrich_prices, listings))

    # Compute marketCapAtListing if we ever have shares (not from 38.co.kr — leave None)
    for r in listings:
        r.setdefault("marketCapAtListing", None)
        # detailNo is internal — keep, useful for direct linking back
        # 38.co.kr doesn't expose listed shares; users can compute via deep-dive

    output = {
        "generatedAt": datetime.now(KST).isoformat(timespec="seconds"),
        "windowFrom": START_DATE,
        "source": "38.co.kr + Naver siseJson",
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
