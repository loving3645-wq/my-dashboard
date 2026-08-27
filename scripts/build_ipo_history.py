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

# 종목명 → 종목코드 자동완성. 38.co.kr 상세페이지가 종목코드를 안 주거나(신규 상장
# 직후 미갱신·503 봇차단) tickers-full.js 마스터에도 없는 신규 상장 종목의 코드를
# 이름으로 직접 해석한다. 반환 code는 siseJson symbol로 그대로 사용 가능
# (신규 상장 종목은 '0039P0' 같은 영숫자 임시코드가 오지만 siseJson이 이를 받는다).
NAVER_AC = "https://ac.stock.naver.com/ac"

# 현재 시가총액 / 현재가. 38.co.kr 상세페이지는 상장주식수를 거의 안 주기 때문에
# 상장 시총이 대부분 비어 있었다 — Naver의 시총(÷현재가 = 내재 주식수)이 사실상
# 모든 상장 종목에 대해 존재하므로 이걸로 현재 시총을 채우고, 최근 상장(주식수
# 변동 미미)에 한해 공모가 × 내재주식수로 상장 시총을 추정한다.
NAVER_INTEGRATION = "https://m.stock.naver.com/api/stock/{}/integration"
# 상장 후 이 기간 이내면 현재 주식수 ≈ 상장 당시 주식수 → 상장 시총 추정 허용.
RECENT_LISTING_DAYS = 550

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
TICKER_MASTER_RE = re.compile(r"'(\d{6})':\s*\{\s*name:\s*'([^']+)',\s*mkt:\s*'([^']+)'")
LISTED_SHARES_RE = re.compile(r"발행주식수[^\d]{0,40}([\d,]+)")
INSTITUTIONAL_LOCKUP_RE = re.compile(r"의무보유확약[^\d]{0,30}(\d+(?:\.\d+)?)\s*%")
EMPLOYEE_SHARES_RE = re.compile(r"우리사주조합[^\d(]{0,40}([\d,]+)\s*주[^()]{0,20}\(\s*(\d+(?:\.\d+)?)\s*%\s*\)")


def load_ticker_master():
    """tickers-full.js에서 정식 KRX 종목 master를 로딩 — SPAC·임시코드 종목의
    name → (ticker, market) fallback 용도."""
    path = os.path.join(ROOT, "tickers-full.js")
    if not os.path.exists(path):
        return {}
    with open(path, "r", encoding="utf-8") as f:
        text = f.read()
    mapping = {}
    for m in TICKER_MASTER_RE.finditer(text):
        ticker, name, mkt = m.group(1), m.group(2), m.group(3)
        mapping[name] = (ticker, mkt)
    return mapping


def normalize_name(name):
    """'채비(구.대영채비)' → '채비'. KRX 마스터 매칭용."""
    return re.sub(r"\s*\(구\.[^)]+\)\s*$", "", name or "").strip()


def naver_ticker_by_name(session, name):
    """종목명으로 Naver 자동완성을 조회해 (code, market)을 돌려준다. 정확히
    일치하는 국내 주식(KOSPI/KOSDAQ) 항목만 채택하고, 없으면 None."""
    clean = normalize_name(name)
    if not clean:
        return None
    try:
        r = session.get(
            NAVER_AC,
            params={"q": clean, "target": "stock"},
            timeout=15,
        )
        if r.status_code != 200:
            return None
        items = r.json().get("items", [])
    except Exception:
        return None
    for it in items:
        code = (it.get("code") or "").strip()
        cand = (it.get("name") or "").strip()
        mkt = (it.get("typeCode") or "").strip().upper()
        if code and cand == clean and mkt in ("KOSPI", "KOSDAQ"):
            return code, mkt
    return None


def fetch_detail(session, detail_no):
    """Detail page → dict with ticker, market, listedShares, institutionalLockupPct,
    employeeShares, employeeSharesPct. None values when not found."""
    out = {
        "ticker": None, "market": None,
        "listedShares": None, "institutionalLockupPct": None,
        "employeeShares": None, "employeeSharesPct": None,
    }
    try:
        r = session.get(DETAIL_URL, params={"o": "v", "no": detail_no}, timeout=15)
        r.encoding = ENC
        if r.status_code != 200:
            return out
        raw = r.text
    except Exception:
        return out
    # ticker / market은 raw에서 (anchor 등이 가까이 있어 OK)
    t = TICKER_RE.search(raw)
    if t:
        out["ticker"] = t.group(1)
    m = MARKET_RE.search(raw)
    if m:
        out["market"] = "KOSDAQ" if m.group(1) in ("코스닥", "KOSDAQ") else "KOSPI"
    # 데이터 필드는 HTML 태그 사이에 끼어있어 strip 후 검색
    stripped = re.sub(r"<[^>]+>", " ", raw)
    stripped = re.sub(r"&nbsp;", " ", stripped)
    stripped = re.sub(r"\s+", " ", stripped)
    ls = LISTED_SHARES_RE.search(stripped)
    if ls:
        try:
            out["listedShares"] = int(ls.group(1).replace(",", ""))
        except ValueError:
            pass
    il = INSTITUTIONAL_LOCKUP_RE.search(stripped)
    if il:
        try:
            out["institutionalLockupPct"] = float(il.group(1))
        except ValueError:
            pass
    es = EMPLOYEE_SHARES_RE.search(stripped)
    if es:
        try:
            out["employeeShares"] = int(es.group(1).replace(",", ""))
            out["employeeSharesPct"] = float(es.group(2))
        except ValueError:
            pass
    return out


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


def fetch_market_cap(session, code):
    """(market_cap_won, current_price) from Naver integration, or (None, None).
    The implied share count (시총 ÷ 현재가) survives 증자/감자, so it is a better
    basis for market cap than the disclosure's coarse share fields."""
    if not code:
        return None, None
    try:
        r = session.get(NAVER_INTEGRATION.format(code), timeout=15)
        if r.status_code != 200:
            return None, None
        infos = {it.get("key"): it.get("value")
                 for it in r.json().get("totalInfos", []) if isinstance(it, dict)}
    except Exception:
        return None, None

    def _won(s):
        if not s:
            return None
        s = s.replace(" ", "").replace(",", "")
        total = 0.0
        jo = re.search(r"([\d.]+)조", s)
        eok = re.search(r"([\d.]+)억", s)
        if jo:
            total += float(jo.group(1)) * 1e12
        if eok:
            total += float(eok.group(1)) * 1e8
        return total or None

    mcap = _won(infos.get("시총"))
    price = infos.get("전일")
    try:
        price = float(price.replace(",", "")) if price and price != "-" else None
    except (ValueError, AttributeError):
        price = None
    return mcap, price


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

    # KRX 종목 마스터 로딩 — SPAC·임시코드 종목의 name → ticker fallback
    ticker_master = load_ticker_master()
    print(f"  ticker master: {len(ticker_master)} names from tickers-full.js", flush=True)

    # Enrich with ticker + market from detail page (+ master fallback)
    print(f"Enriching {len(listings)} with ticker/market from detail pages...", flush=True)
    completed = [0]
    fallback_hits = [0]

    def enrich_detail(ipo):
        info = fetch_detail(sess, ipo["detailNo"])
        ticker = info["ticker"]
        market = info["market"]
        if not ticker:
            # Fallback — name으로 KRX master lookup. 38.co.kr이 SPAC 식별자
            # ('0129K0')나 임시 등록번호('0011T0')만 보여주는 케이스.
            clean = normalize_name(ipo["name"])
            hit = ticker_master.get(clean)
            if hit:
                ticker, master_mkt = hit
                if not market:
                    market = master_mkt
                fallback_hits[0] += 1
            else:
                # 3차 폴백 — Naver 자동완성으로 신규 상장 종목 코드 해석.
                # 정적 master(tickers-full.js)에 아직 없는 최신 상장이 여기서 잡힌다.
                ac = naver_ticker_by_name(sess, ipo["name"])
                if ac:
                    ticker, ac_mkt = ac
                    if not market:
                        market = ac_mkt
                    fallback_hits[0] += 1
        if ticker:
            ipo["ticker"] = ticker
        if market:
            ipo["market"] = market
        # 38.co.kr detail에서 추출한 정확 데이터 — 종목별 락업/물량 정보
        for k in ("listedShares", "institutionalLockupPct",
                  "employeeShares", "employeeSharesPct"):
            v = info.get(k)
            if v is not None:
                ipo[k] = v
        # marketCapAtListing이 비어있으면 listedShares × ipoPrice 로 재계산
        if not ipo.get("marketCapAtListing") and ipo.get("listedShares") and ipo.get("ipoPrice"):
            ipo["marketCapAtListing"] = ipo["listedShares"] * ipo["ipoPrice"]
        completed[0] += 1
        if completed[0] % 100 == 0:
            print(f"  detail {completed[0]}/{len(listings)} (fallback={fallback_hits[0]})", flush=True)
        return ipo

    with ThreadPoolExecutor(max_workers=DETAIL_WORKERS) as ex:
        listings = list(ex.map(enrich_detail, listings))
    with_ticker = sum(1 for r in listings if r.get("ticker"))
    print(f"  fallback name-lookup hits: {fallback_hits[0]}", flush=True)
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

    # Current market cap (+ listing-time estimate) from Naver — 38.co.kr rarely
    # exposes listed shares, so 상장 시총 was mostly empty. One integration call
    # per distinct ticker; delisted/merged SPACs simply return nothing.
    print("Enriching with Naver market cap...", flush=True)
    mkt = make_session()
    today = datetime.now(KST).date()
    codes = sorted({r["ticker"] for r in listings if r.get("ticker")})

    def fetch_mc(code):
        return code, fetch_market_cap(mkt, code)

    quote = {}
    completed[0] = 0
    with ThreadPoolExecutor(max_workers=PRICE_WORKERS) as ex:
        for code, mc in ex.map(fetch_mc, codes):
            quote[code] = mc
            completed[0] += 1
            if completed[0] % 200 == 0:
                print(f"  mktcap {completed[0]}/{len(codes)}", flush=True)

    filled_cur = filled_est = 0
    for r in listings:
        mc, px = quote.get(r.get("ticker"), (None, None))
        if mc and mc > 0:
            r["currentMarketCap"] = int(round(mc))   # 원
            filled_cur += 1
        if px and px > 0:
            r["currentPrice"] = int(round(px))
            ipo_p = r.get("ipoPrice")
            if isinstance(ipo_p, (int, float)) and ipo_p > 0:
                r["currentReturn"] = round((px / ipo_p - 1) * 100, 1)
        # Listing-time market cap estimate for recent IPOs lacking exact shares.
        if not r.get("marketCapAtListing") and mc and px and px > 0:
            try:
                age = (today - datetime.strptime(r.get("listingDate", ""), "%Y-%m-%d").date()).days
            except ValueError:
                age = 10 ** 9
            ipo_p = r.get("ipoPrice")
            if age <= RECENT_LISTING_DAYS and isinstance(ipo_p, (int, float)) and ipo_p > 0:
                r["marketCapAtListing"] = int(round(ipo_p * (mc / px)))
                r["marketCapAtListingEst"] = True
                filled_est += 1
    print(f"  current market cap: {filled_cur} · listing-time estimate: {filled_est}", flush=True)

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
