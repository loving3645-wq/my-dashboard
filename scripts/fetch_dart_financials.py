#!/usr/bin/env python3
"""3-year annual financials for every KOSPI/KOSDAQ ticker via OpenDART.

Pipeline:
  1) Load tickers from tickers-full.js (KOSPI/KOSDAQ corporate stocks).
  2) Download OpenDART corpCode.xml master, build stock_code → corp_code map.
  3) For each ticker, call fnlttSinglAcntAll (annual, 사업보고서) and pull
     매출액 / 매출원가 / 매출총이익 / 영업이익 / 당기순이익 across 3 fiscal years.
  4) Compute margins + revenue growth, emit data/financials.json.

The frontend (stock-deep-dive.html) loads the JSON and replaces the
"데이터 준비 중" placeholders with real DART numbers.

Run weekly — annual filings only update around March each year.
"""

import io
import json
import os
import re
import sys
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from xml.etree import ElementTree as ET

import requests

KST = timezone(timedelta(hours=9))

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TICKERS_FILE = os.path.join(ROOT, "tickers-full.js")
OUTPUT_FILE = os.path.join(ROOT, "data", "financials.json")

DART_KEY = os.environ.get("OPENDART_KEY", "").strip()
CORP_CODE_URL = "https://opendart.fss.or.kr/api/corpCode.xml"
ACNT_URL = "https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json"

REPRT_ANNUAL = "11011"  # 사업보고서

WORKERS = 8
TIMEOUT = 15
RETRY_COUNT = 2

# As of run-time, query this fiscal year first (most recent annual report
# available). Each call returns thstrm/frmtrm/bfefrmtrm = 3 years. If the
# response is empty (filing not yet published for small caps), step back one
# year.
def latest_fiscal_year():
    today = datetime.now(KST)
    # Annual reports are due by end of March for prior FY. If we're past
    # April, FY-1 should be available; otherwise FY-2 is the safer baseline.
    return today.year - 1 if today.month >= 4 else today.year - 2


# Account matching. Primary key is account_id (standard XBRL element);
# account_nm is the fallback for corporates that report with non-standard
# IDs. Order within each list is preference order.
ACCOUNT_KEYS = {
    "revenue": {
        "ids": ["ifrs-full_Revenue", "ifrs_Revenue"],
        "names": ["매출액", "수익(매출액)", "영업수익"],
    },
    "cogs": {
        "ids": ["ifrs-full_CostOfSales", "ifrs_CostOfSales"],
        "names": ["매출원가"],
    },
    "grossProfit": {
        "ids": ["ifrs-full_GrossProfit", "ifrs_GrossProfit"],
        "names": ["매출총이익"],
    },
    "operatingIncome": {
        "ids": [
            "dart_OperatingIncomeLoss",
            "ifrs-full_ProfitLossFromOperatingActivities",
        ],
        "names": ["영업이익", "영업이익(손실)"],
    },
    "netIncome": {
        "ids": ["ifrs-full_ProfitLoss", "ifrs_ProfitLoss"],
        "names": ["당기순이익", "당기순이익(손실)"],
    },
}


def make_session():
    s = requests.Session()
    s.headers.update({"User-Agent": "my-dashboard/1.0 (financials cron)"})
    return s


def load_tickers():
    """Return ordered list of (ticker, name) from tickers-full.js."""
    with open(TICKERS_FILE, encoding="utf-8") as f:
        text = f.read()
    out = []
    for m in re.finditer(
        r"'([0-9A-Z]{6})':\s*\{\s*name:\s*'([^']+)'", text
    ):
        out.append((m.group(1), m.group(2)))
    return out


def download_corp_code_map(sess):
    """Fetch DART corpCode.xml.zip and return {stock_code: corp_code}."""
    r = sess.get(CORP_CODE_URL, params={"crtfc_key": DART_KEY}, timeout=30)
    r.raise_for_status()
    zf = zipfile.ZipFile(io.BytesIO(r.content))
    xml_bytes = zf.read(zf.namelist()[0])
    root = ET.fromstring(xml_bytes)
    mapping = {}
    for elt in root.findall("list"):
        stock_code = (elt.findtext("stock_code") or "").strip()
        corp_code = (elt.findtext("corp_code") or "").strip()
        if stock_code and corp_code:
            mapping[stock_code] = corp_code
    return mapping


def _matches(row, spec):
    aid = (row.get("account_id") or "").strip()
    if aid in spec["ids"]:
        return True
    nm = (row.get("account_nm") or "").strip()
    return nm in spec["names"]


def _amount(row, key):
    raw = (row.get(key) or "").replace(",", "").strip()
    if not raw or raw == "-":
        return None
    try:
        return float(raw)
    except ValueError:
        return None


def parse_acnt_response(rows):
    """Extract 3-year time series in 조원 from a fnlttSinglAcntAll list.

    Returns dict with year-aligned arrays, or None if revenue is missing
    (we treat absence of 매출액 as "no usable data" since the whole UI
    keys off it)."""
    # Pick 손익계산서 rows preferentially. DART labels: 'IS' (손익계산서) or
    # 'CIS' (포괄손익계산서). When a company files only CIS, use that.
    is_rows = [r for r in rows if (r.get("sj_div") or "") in ("IS", "CIS")]
    if not is_rows:
        return None

    # Grab fiscal years from the header row. Every row carries the same
    # thstrm_nm/frmtrm_nm/bfefrmtrm_nm — read from the first.
    head = is_rows[0]

    def year_from(label):
        # label looks like "제 56 기" or "2024.01.01 ~ 2024.12.31".
        # The thstrm_dt field (e.g. "2024.01.01 ~ 2024.12.31") is the
        # reliable source for the fiscal year.
        if not label:
            return None
        m = re.search(r"(\d{4})", label)
        return m.group(1) if m else None

    y_curr = year_from(head.get("thstrm_dt") or head.get("thstrm_nm"))
    y_prev = year_from(head.get("frmtrm_dt") or head.get("frmtrm_nm"))
    y_prev2 = year_from(head.get("bfefrmtrm_dt") or head.get("bfefrmtrm_nm"))
    years = [y for y in (y_prev2, y_prev, y_curr) if y]
    if len(years) < 2:
        return None

    out = {k: [None] * len(years) for k in ACCOUNT_KEYS}
    amount_cols = ["bfefrmtrm_amount", "frmtrm_amount", "thstrm_amount"]
    # Align amount_cols index with `years` (which we built from oldest to
    # newest, dropping any missing). Re-derive index map from year labels.
    col_for_year = {
        y_prev2: "bfefrmtrm_amount",
        y_prev: "frmtrm_amount",
        y_curr: "thstrm_amount",
    }
    cols = [col_for_year[y] for y in years]

    for key, spec in ACCOUNT_KEYS.items():
        for row in is_rows:
            if _matches(row, spec):
                for i, col in enumerate(cols):
                    if out[key][i] is None:
                        v = _amount(row, col)
                        if v is not None:
                            out[key][i] = v
                # Don't break — multiple rows may match; later ones can
                # fill gaps left by earlier rows. But if every slot is
                # filled, stop.
                if all(v is not None for v in out[key]):
                    break

    if not any(v is not None for v in out["revenue"]):
        return None

    # Convert KRW → 조원 (1조 = 1e12 KRW). Round to 2 decimals.
    def to_jo(v):
        return round(v / 1e12, 2) if v is not None else None

    for k in ACCOUNT_KEYS:
        out[k] = [to_jo(v) for v in out[k]]

    # Margins (%). Guard against None and zero revenue.
    def pct(num, den):
        if num is None or den is None or den == 0:
            return None
        return round(num / den * 100, 1)

    grossMargin = []
    opMargin = []
    netMargin = []
    for i in range(len(years)):
        r = out["revenue"][i]
        # Some filers report 매출원가 + 매출총이익 explicitly; others omit
        # 매출총이익 and we compute it. Same for the reverse.
        gp = out["grossProfit"][i]
        if gp is None and out["cogs"][i] is not None and r is not None:
            gp = round(r - out["cogs"][i], 2)
            out["grossProfit"][i] = gp
        grossMargin.append(pct(gp, r))
        opMargin.append(pct(out["operatingIncome"][i], r))
        netMargin.append(pct(out["netIncome"][i], r))

    revGrowth = [None]
    for i in range(1, len(years)):
        prev = out["revenue"][i - 1]
        curr = out["revenue"][i]
        if prev and curr and prev != 0:
            revGrowth.append(round((curr / prev - 1) * 100, 1))
        else:
            revGrowth.append(None)

    return {
        "years": years,
        "revenue": out["revenue"],
        "cogs": out["cogs"],
        "grossProfit": out["grossProfit"],
        "operatingIncome": out["operatingIncome"],
        "netIncome": out["netIncome"],
        "grossMargin": grossMargin,
        "opMargin": opMargin,
        "netMargin": netMargin,
        "revGrowth": revGrowth,
    }


def fetch_one(sess, ticker, name, corp_code, base_year):
    """Try CFS first, then OFS; try base_year, then base_year-1."""
    last_error = None
    for fs_div in ("CFS", "OFS"):
        for year_offset in (0, 1):
            year = base_year - year_offset
            params = {
                "crtfc_key": DART_KEY,
                "corp_code": corp_code,
                "bsns_year": str(year),
                "reprt_code": REPRT_ANNUAL,
                "fs_div": fs_div,
            }
            for attempt in range(RETRY_COUNT + 1):
                try:
                    r = sess.get(ACNT_URL, params=params, timeout=TIMEOUT)
                    if r.status_code != 200:
                        last_error = f"HTTP {r.status_code}"
                        break
                    body = r.json()
                    status = body.get("status")
                    # 000 = OK, 013 = no data (skip silently),
                    # 020 = key invalid, 100/101/800/900 = transient
                    if status == "013":
                        last_error = "no-data"
                        break
                    if status != "000":
                        if status in ("100", "101", "800", "900"):
                            time.sleep(0.5 * (attempt + 1))
                            continue
                        last_error = f"status={status}"
                        break
                    rows = body.get("list") or []
                    parsed = parse_acnt_response(rows)
                    if parsed:
                        return {
                            "name": name,
                            "corpCode": corp_code,
                            "fsDiv": fs_div,
                            "fiscalYear": int(parsed["years"][-1]),
                            **parsed,
                        }
                    last_error = "parse-empty"
                    break
                except requests.RequestException as e:
                    last_error = f"net:{e.__class__.__name__}"
                    time.sleep(0.5 * (attempt + 1))
                    continue
    return None


def main():
    if not DART_KEY:
        print("ERROR: OPENDART_KEY env var not set", file=sys.stderr)
        sys.exit(1)

    sess = make_session()
    print("Fetching DART corp_code master…", flush=True)
    code_map = download_corp_code_map(sess)
    print(f"  loaded {len(code_map):,} listed corp_code entries", flush=True)

    tickers = load_tickers()
    print(f"Loaded {len(tickers):,} tickers from tickers-full.js", flush=True)

    base_year = latest_fiscal_year()
    print(f"Querying base fiscal year {base_year}…", flush=True)

    results = {}
    skipped_no_corp = 0
    skipped_no_data = 0

    def task(ticker, name):
        corp_code = code_map.get(ticker)
        if not corp_code:
            return ticker, None, "no_corp"
        fin = fetch_one(sess, ticker, name, corp_code, base_year)
        if fin is None:
            return ticker, None, "no_data"
        return ticker, fin, None

    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futures = {ex.submit(task, t, n): t for t, n in tickers}
        done = 0
        total = len(futures)
        for fut in as_completed(futures):
            done += 1
            ticker, fin, reason = fut.result()
            if fin is not None:
                results[ticker] = fin
            elif reason == "no_corp":
                skipped_no_corp += 1
            else:
                skipped_no_data += 1
            if done % 200 == 0:
                print(
                    f"  {done}/{total} · ok={len(results)} "
                    f"no_corp={skipped_no_corp} no_data={skipped_no_data}",
                    flush=True,
                )

    payload = {
        "asOf": datetime.now(KST).isoformat(timespec="seconds"),
        "fiscalYearBase": base_year,
        "count": len(results),
        "skippedNoCorp": skipped_no_corp,
        "skippedNoData": skipped_no_data,
        "financials": results,
    }
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, separators=(",", ":"))

    print(
        f"Wrote {OUTPUT_FILE} · ok={len(results)} "
        f"no_corp={skipped_no_corp} no_data={skipped_no_data}",
        flush=True,
    )


if __name__ == "__main__":
    main()
