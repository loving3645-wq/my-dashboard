#!/usr/bin/env python3
"""Quarterly + annual earnings for every KOSPI/KOSDAQ ticker via OpenDART.

Why this is a bulk pipeline
---------------------------
The previous version called fnlttSinglAcntAll once per ticker (up to four
attempts each: CFS/OFS x two fiscal years). That is ~10,000 HTTP calls per
run, and it never once finished: all 29 scheduled runs were killed by the
job's 30-minute timeout, which skips the commit step, so data/financials.json
was never produced at all.

This version uses fnlttMultiAcnt (다중회사 주요계정), which takes up to 99
corp_codes per call. ~2,600 tickers x 4 report codes x 2 base years is about
210 calls instead of 10,000 — a couple of minutes instead of never.

The trade-off: 주요계정 carries 매출액 / 영업이익 / 당기순이익 but not
매출원가 / 매출총이익, so those stay null. For earnings tracking that is the
right trade: the three headline lines, every quarter, for the whole market.

Report codes map to quarters: 11013 -> Q1, 11012 -> 반기(Q2 누적),
11014 -> Q3 누적, 11011 -> 사업보고서(FY 누적).

Units
-----
Amounts are stored in 억원, not 조원. Most of the market is small caps; at
조원 with two decimals a 500억 revenue rounds to 0.0 and the number is
useless. 억원 keeps every listed company legible.

Output: data/financials.json
"""

import io
import json
import os
import re
import signal
import sys
import threading
import time
import zipfile
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from xml.etree import ElementTree as ET

import requests

KST = timezone(timedelta(hours=9))

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TICKERS_FILE = os.path.join(ROOT, "tickers-full.js")
OUTPUT_FILE = os.path.join(ROOT, "data", "financials.json")
# The corpCode.xml master is a ~4,000-entry zip that took 3m56s of a
# 20-minute budget on the first real run — a fifth of the run spent before
# any work started. The mapping only changes when a company lists or
# delists, so it is cached and refreshed weekly.
CORP_CACHE_FILE = os.path.join(ROOT, "data", "dart-corpcode.json")
CORP_CACHE_MAX_AGE_DAYS = 7

DART_KEY = os.environ.get("OPENDART_KEY", "").strip()
CORP_CODE_URL = "https://opendart.fss.or.kr/api/corpCode.xml"
MULTI_ACNT_URL = "https://opendart.fss.or.kr/api/fnlttMultiAcnt.json"
LIST_URL = "https://opendart.fss.or.kr/api/list.json"

# DART caps corp_code at 99 per fnlttMultiAcnt call.
BATCH_SIZE = 99

# How many fiscal years back to query. Each call also returns the prior
# year's same period (frmtrm), so 2 query years yield ~3 years of history.
QUERY_YEARS = int(os.environ.get("FIN_QUERY_YEARS", "2"))

# Wall-clock budget, deliberately shorter than the job's timeout-minutes.
# A run that overruns writes what it has and exits 0 rather than being
# killed with its commit step skipped.
BUDGET_SECONDS = int(float(os.environ.get("FIN_BUDGET_MIN", "25")) * 60)

WORKERS = 10
TIMEOUT = 20
RETRY_COUNT = 3

# reprt_code -> (quarter number, label)
REPORTS = {
    "11013": (1, "1분기보고서"),
    "11012": (2, "반기보고서"),
    "11014": (3, "3분기보고서"),
    "11011": (4, "사업보고서"),
}

# 주요계정 only exposes account_nm (no account_id), so matching is by name.
# Filers vary: 수익(매출액) and 영업수익 are both 매출액 for our purposes.
ACCOUNT_NAMES = {
    "revenue": ["매출액", "수익(매출액)", "영업수익"],
    "operatingIncome": ["영업이익", "영업이익(손실)"],
    "netIncome": ["당기순이익", "당기순이익(손실)"],
}

HARD_FAIL_STATUSES = frozenset({"010", "011", "012", "014"})
TRANSIENT_STATUSES = frozenset({"100", "101", "800", "900"})
RATELIMIT_STATUSES = frozenset({"020"})

_API_FAILURES = Counter()
_FAIL_LOCK = threading.Lock()
_DIAGS = Counter()
_DIAG_LOCK = threading.Lock()


def _fail(kind):
    with _FAIL_LOCK:
        _API_FAILURES[kind] += 1


def _diag(kind, n=1):
    with _DIAG_LOCK:
        _DIAGS[kind] += n


_DEADLINE = None


def _arm(at):
    global _DEADLINE
    _DEADLINE = at


def _expired():
    return _DEADLINE is not None and time.monotonic() >= _DEADLINE


def _on_signal(signum, frame):
    print(f"Signal {signum} — winding down, writing partial results", flush=True)
    _arm(time.monotonic())


def _sleep(seconds):
    """Backoff that gives up as soon as the budget is spent, so a DART
    outage degrades the run instead of stalling it past the job timeout."""
    if _expired():
        return False
    time.sleep(seconds)
    return not _expired()


def make_session():
    s = requests.Session()
    s.headers.update({"User-Agent": "my-dashboard/1.0 (financials cron)"})
    return s


def load_tickers():
    with open(TICKERS_FILE, encoding="utf-8") as f:
        text = f.read()
    out = []
    for m in re.finditer(r"'([0-9A-Z]{6})':\s*\{\s*name:\s*'([^']+)'", text):
        out.append((m.group(1), m.group(2)))
    return out


def load_corp_code_cache():
    """Cached {stock_code: corp_code}, or None when missing or stale."""
    try:
        with open(CORP_CACHE_FILE, encoding="utf-8") as f:
            data = json.load(f)
    except (FileNotFoundError, ValueError):
        return None
    mapping = data.get("map")
    if not isinstance(mapping, dict) or not mapping:
        return None
    try:
        age = datetime.now(KST) - datetime.fromisoformat(data["asOf"])
    except (KeyError, TypeError, ValueError):
        return None
    if age > timedelta(days=CORP_CACHE_MAX_AGE_DAYS):
        return None
    return mapping


def save_corp_code_cache(mapping):
    os.makedirs(os.path.dirname(CORP_CACHE_FILE), exist_ok=True)
    tmp = CORP_CACHE_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(
            {"asOf": datetime.now(KST).isoformat(timespec="seconds"),
             "count": len(mapping), "map": mapping},
            f, ensure_ascii=False, separators=(",", ":"), sort_keys=True,
        )
    os.replace(tmp, CORP_CACHE_FILE)


def download_corp_code_map(sess):
    r = sess.get(CORP_CODE_URL, params={"crtfc_key": DART_KEY}, timeout=60)
    r.raise_for_status()
    # DART answers HTTP 200 with a JSON error body (not a zip) when the key
    # is invalid, expired or over quota. Say so instead of raising BadZipFile.
    if not r.content.startswith(b"PK"):
        try:
            body = r.json()
            raise RuntimeError(
                f"corpCode.xml not a zip — DART status={body.get('status')!r} "
                f"message={body.get('message')!r}"
            )
        except ValueError:
            raise RuntimeError(
                f"corpCode.xml not a zip — first 200 bytes: {r.content[:200]!r}"
            )
    zf = zipfile.ZipFile(io.BytesIO(r.content))
    root = ET.fromstring(zf.read(zf.namelist()[0]))
    mapping = {}
    for elt in root.findall("list"):
        sc = (elt.findtext("stock_code") or "").strip()
        cc = (elt.findtext("corp_code") or "").strip()
        if sc and cc:
            mapping[sc] = cc
    return mapping


def _amount(row, key):
    raw = (row.get(key) or "").replace(",", "").strip()
    if not raw or raw in ("-", "―"):
        return None
    neg = raw.startswith("(") and raw.endswith(")")
    if neg:
        raw = raw[1:-1]
    try:
        v = float(raw)
    except ValueError:
        return None
    return -v if neg else v


def _account_key(row):
    nm = (row.get("account_nm") or "").strip()
    for key, names in ACCOUNT_NAMES.items():
        if nm in names:
            return key
    return None


_DATE_RANGE_RE = re.compile(r"(\d{4})\.(\d{2})\.(\d{2})\s*~\s*(\d{4})\.(\d{2})\.(\d{2})")


def _span_months(dt_label):
    """Months covered by a DART period label like '2025.01.01 ~ 2025.09.30'.

    This is how we tell a cumulative figure from a three-month one without
    guessing: a 반기/3분기 주요계정 row that starts on Jan 1 is year-to-date.
    Returns (months, end_year) or (None, None) when the label is unparseable.
    """
    if not dt_label:
        return None, None
    m = _DATE_RANGE_RE.search(dt_label)
    if not m:
        # Some rows carry only an end date (재무상태표 style).
        m2 = re.search(r"(\d{4})\.(\d{2})\.(\d{2})", dt_label)
        return None, (int(m2.group(1)) if m2 else None)
    y1, m1, _d1, y2, m2_, _d2 = (int(g) for g in m.groups())
    months = (y2 - y1) * 12 + (m2_ - m1) + 1
    return months, y2


def fetch_batch(sess, corp_codes, year, reprt_code):
    """One fnlttMultiAcnt call. Returns (rows, ok)."""
    params = {
        "crtfc_key": DART_KEY,
        "corp_code": ",".join(corp_codes),
        "bsns_year": str(year),
        "reprt_code": reprt_code,
    }
    last = "network"
    for attempt in range(RETRY_COUNT + 1):
        try:
            r = sess.get(MULTI_ACNT_URL, params=params, timeout=TIMEOUT)
            if r.status_code == 429 or 500 <= r.status_code < 600:
                last = "ratelimit" if r.status_code == 429 else "http_error"
                if not _sleep(2 * (attempt + 1)):
                    break
                continue
            if r.status_code != 200:
                _fail("http_error")
                return [], False
            body = r.json()
            status = body.get("status")
            if status == "000":
                return body.get("list") or [], True
            if status == "013":
                # No filings for this (year, report) across the whole batch.
                return [], True
            if status in HARD_FAIL_STATUSES:
                _fail("http_error")
                return [], False
            if status in RATELIMIT_STATUSES:
                last = "ratelimit"
                if not _sleep(2 ** (attempt + 1)):
                    break
                continue
            if status in TRANSIENT_STATUSES:
                last = "transient"
                if not _sleep(0.5 * (attempt + 1)):
                    break
                continue
            _fail("http_error")
            return [], False
        except (requests.RequestException, ValueError):
            last = "network"
            if not _sleep(0.5 * (attempt + 1)):
                break
            continue
    _fail(last)
    return [], False


def collect_periods(sess, corp_codes, years, by_corp):
    """Fill by_corp[corp][(year, quarter)][metric] = (value, span_months).

    Each call also carries the prior year's same period (frmtrm), so two
    query years give roughly three years of quarters.
    """
    batches = [
        corp_codes[i:i + BATCH_SIZE]
        for i in range(0, len(corp_codes), BATCH_SIZE)
    ]
    jobs = [
        (batch, year, code)
        for year in years
        for code in REPORTS
        for batch in batches
    ]
    lock = threading.Lock()
    done = 0
    total = len(jobs)

    def run(job):
        batch, year, code = job
        if _expired():
            return "budget", None
        rows, ok = fetch_batch(sess, batch, year, code)
        return ("ok" if ok else "fail"), (rows, year, code)

    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futures = {ex.submit(run, j): j for j in jobs}
        for fut in as_completed(futures):
            outcome, payload = fut.result()
            with lock:
                done += 1
                _diag(outcome)
                if outcome == "ok" and payload:
                    rows, year, code = payload
                    quarter = REPORTS[code][0]
                    _ingest(rows, year, quarter, by_corp)
                if done % 40 == 0 or done == total:
                    print(
                        f"  {done}/{total} calls · ok={_DIAGS['ok']} "
                        f"fail={_DIAGS['fail']} budget_skip={_DIAGS['budget']}",
                        flush=True,
                    )


def _ingest(rows, year, quarter, by_corp):
    """Pull 매출/영업이익/순이익 out of a batch response.

    Consolidated (CFS) wins over standalone (OFS) when a filer reports both;
    the frontend shows group numbers everywhere else.
    """
    for row in rows:
        key = _account_key(row)
        if key is None:
            continue
        corp = (row.get("corp_code") or "").strip()
        if not corp:
            continue
        prefer_cfs = (row.get("fs_div") or "").strip().upper() == "CFS"
        for col_amt, col_dt, yr in (
            ("thstrm_amount", "thstrm_dt", year),
            ("frmtrm_amount", "frmtrm_dt", year - 1),
        ):
            val = _amount(row, col_amt)
            if val is None:
                continue
            months, end_year = _span_months(row.get(col_dt))
            slot = by_corp[corp][(end_year or yr, quarter)]
            prev = slot.get(key)
            # First writer wins unless this row is consolidated and the
            # stored one was standalone.
            if prev is None or (prefer_cfs and not prev[2]):
                slot[key] = (val, months, prefer_cfs)


def derive_quarters(periods):
    """Turn each report's figures into discrete quarters.

    DART mixes the two shapes. In the first full run 20,116 account rows
    carried a year-to-date span and 17,101 carried a three-month span, so
    neither can be assumed — each row's own period label decides.

    The subtlety that matters: when a year-to-date row IS differenced, it
    must be differenced against the cumulative total of the quarters
    already resolved, not against the previous row's raw value. The
    previous row may itself have been a three-month figure, and
    `cumulative - three_month` is a meaningless number. So we walk Q1..Q4
    carrying a running total of resolved quarters and subtract that.

    A gap in the ladder (a quarter we have no row for) makes every later
    year-to-date figure underivable, so the running total is marked
    untrustworthy rather than silently reused. A difference that would
    make revenue negative means the two reports disagree (restatement,
    or a filer whose labels lie), so that quarter is dropped.
    """
    by_year = {}
    for (year, q), metrics in periods.items():
        if year is None:
            continue
        by_year.setdefault(year, {})[q] = metrics

    quarters = []
    for year in sorted(by_year):
        byq = by_year[year]
        resolved = {k: {} for k in ACCOUNT_NAMES}
        for key in ACCOUNT_NAMES:
            running = 0.0
            have_running = True
            for q in (1, 2, 3, 4):
                cell = byq.get(q, {}).get(key)
                if cell is None:
                    # No row for this quarter: later YTD figures can no
                    # longer be differenced against a known total.
                    have_running = False
                    continue
                val, months, _cfs = cell
                # Q1 is three months either way, so it needs no differencing.
                is_discrete = q == 1 or (months is not None and months <= 4)
                if is_discrete:
                    value = val
                    _diag("span_q1" if q == 1 else "span_discrete")
                else:
                    if not have_running:
                        _diag("missing_prev_quarter")
                        continue
                    value = val - running
                    if key == "revenue" and value < 0:
                        _diag("negative_derived_revenue")
                        have_running = False
                        continue
                    _diag("span_cumulative")
                resolved[key][q] = value
                if have_running:
                    running += value

        for q in (1, 2, 3, 4):
            rec = {"year": year, "quarter": q}
            usable = False
            for key in ACCOUNT_NAMES:
                v = resolved[key].get(q)
                rec[key] = v
                if v is not None:
                    usable = True
            if usable:
                quarters.append(rec)
    return quarters


def to_eok(v):
    """KRW -> 억원, one decimal. 억원 keeps micro caps readable where the
    previous 조원 scale rounded most of KOSDAQ to zero."""
    return round(v / 1e8, 1) if v is not None else None


def pct(num, den):
    if num is None or den in (None, 0):
        return None
    return round(num / den * 100, 1)


def build_entry(name, corp_code, periods):
    quarters = derive_quarters(periods)

    # Annual series: prefer the 사업보고서 cumulative (q=4), which is the
    # full-year figure, rather than summing derived quarters.
    annual = {}
    for (year, q), metrics in periods.items():
        if q != 4 or year is None:
            continue
        annual[year] = {
            k: (metrics[k][0] if metrics.get(k) else None)
            for k in ACCOUNT_NAMES
        }

    years = sorted(annual)[-3:]
    rev = [to_eok(annual[y]["revenue"]) for y in years]
    op = [to_eok(annual[y]["operatingIncome"]) for y in years]
    net = [to_eok(annual[y]["netIncome"]) for y in years]

    rev_growth = [None]
    for i in range(1, len(years)):
        p, c = rev[i - 1], rev[i]
        rev_growth.append(round((c / p - 1) * 100, 1) if p and c and p != 0 else None)

    q_out = []
    by_key = {(r["year"], r["quarter"]): r for r in quarters}
    for r in quarters:
        prev_year = by_key.get((r["year"] - 1, r["quarter"]))
        revenue = to_eok(r["revenue"])
        opi = to_eok(r["operatingIncome"])
        entry = {
            "period": f"{r['year']}Q{r['quarter']}",
            "year": r["year"],
            "quarter": r["quarter"],
            "revenue": revenue,
            "operatingIncome": opi,
            "netIncome": to_eok(r["netIncome"]),
            "opMargin": pct(opi, revenue),
            "revYoY": None,
            "opYoY": None,
            "turnaround": None,
        }
        if prev_year:
            pr = to_eok(prev_year["revenue"])
            po = to_eok(prev_year["operatingIncome"])
            if pr and revenue is not None and pr != 0:
                entry["revYoY"] = round((revenue / pr - 1) * 100, 1)
            if po is not None and opi is not None:
                if po != 0 and (po > 0) == (opi > 0):
                    entry["opYoY"] = round((opi / abs(po) - 1) * 100, 1) if po > 0 else None
                # Sign flips are the headline, not a percentage.
                if po < 0 <= opi:
                    entry["turnaround"] = "흑자전환"
                elif po >= 0 > opi:
                    entry["turnaround"] = "적자전환"
        q_out.append(entry)

    q_out.sort(key=lambda e: (e["year"], e["quarter"]))

    if not any(v is not None for v in rev) and not q_out:
        return None

    return {
        "name": name,
        "corpCode": corp_code,
        # Annual block keeps the shape stock-deep-dive.html already reads.
        "years": [str(y) for y in years],
        "revenue": rev,
        "cogs": [None] * len(years),
        "grossProfit": [None] * len(years),
        "operatingIncome": op,
        "netIncome": net,
        "grossMargin": [None] * len(years),
        "opMargin": [pct(op[i], rev[i]) for i in range(len(years))],
        "netMargin": [pct(net[i], rev[i]) for i in range(len(years))],
        "revGrowth": rev_growth,
        "quarters": q_out[-12:],
        "latest": q_out[-1] if q_out else None,
    }


def fetch_report_filings(sess, days=120):
    """Periodic-report filings (정기공시) market-wide, for 실적 발표일.

    fnlttMultiAcnt carries no receipt date, so when a result was actually
    announced comes from the disclosure list instead. pblntf_ty=A is 정기공시
    (사업/반기/분기보고서), which is exactly the set we want, and one page
    carries 100 filings — the whole window is a few dozen calls.
    """
    today = datetime.now(KST).date()
    bgn = (today - timedelta(days=days)).strftime("%Y%m%d")
    end = today.strftime("%Y%m%d")
    out = {}
    pages = 0
    for corp_cls in ("Y", "K"):
        total_page = None
        for page_no in range(1, 60):
            if _expired():
                _diag("filings_budget_stop")
                return out, pages
            params = {
                "crtfc_key": DART_KEY,
                "bgn_de": bgn,
                "end_de": end,
                "corp_cls": corp_cls,
                "pblntf_ty": "A",
                "page_count": 100,
                "page_no": page_no,
            }
            body = None
            for attempt in range(RETRY_COUNT + 1):
                try:
                    r = sess.get(LIST_URL, params=params, timeout=TIMEOUT)
                    if r.status_code != 200:
                        break
                    j = r.json()
                    st = j.get("status")
                    if st == "000":
                        body = j
                        break
                    if st == "013":
                        body = {"list": [], "total_page": 0}
                        break
                    if st in RATELIMIT_STATUSES or st in TRANSIENT_STATUSES:
                        if not _sleep(1.5 * (attempt + 1)):
                            break
                        continue
                    break
                except (requests.RequestException, ValueError):
                    if not _sleep(0.5 * (attempt + 1)):
                        break
                    continue
            if body is None:
                _diag("filings_page_failed")
                break
            pages += 1
            rows = body.get("list") or []
            for row in rows:
                code = (row.get("stock_code") or "").strip()
                if len(code) != 6:
                    continue
                title = (row.get("report_nm") or "").strip()
                if not re.search(r"분기보고서|반기보고서|사업보고서", title):
                    continue
                dt = (row.get("rcept_dt") or "").strip()
                iso = f"{dt[:4]}-{dt[4:6]}-{dt[6:8]}" if len(dt) == 8 else None
                prev = out.get(code)
                if prev is None or (iso or "") > (prev.get("date") or ""):
                    out[code] = {
                        "date": iso,
                        "title": title,
                        "rceptNo": (row.get("rcept_no") or "").strip() or None,
                    }
            if total_page is None:
                try:
                    total_page = int(body.get("total_page") or 1)
                except (TypeError, ValueError):
                    total_page = 1
            if page_no >= total_page or not rows:
                break
    return out, pages


def load_existing():
    try:
        with open(OUTPUT_FILE, encoding="utf-8") as f:
            data = json.load(f)
    except (FileNotFoundError, ValueError):
        return {}
    return data if isinstance(data, dict) else {}


def main():
    if not DART_KEY:
        print("ERROR: OPENDART_KEY env var not set", file=sys.stderr)
        sys.exit(1)

    started = time.monotonic()
    _arm(started + BUDGET_SECONDS)
    signal.signal(signal.SIGTERM, _on_signal)
    signal.signal(signal.SIGINT, _on_signal)
    print(f"Budget {BUDGET_SECONDS // 60} min · {QUERY_YEARS} query years",
          flush=True)

    sess = make_session()
    previous = load_existing()
    prev_fin = previous.get("financials") or {}
    print(f"Existing data: {len(prev_fin):,} tickers "
          f"(asOf {previous.get('asOf') or '—'})", flush=True)

    code_map = load_corp_code_cache()
    if code_map:
        print(f"  corp_code from cache: {len(code_map):,} entries", flush=True)
    else:
        print("Fetching DART corp_code master…", flush=True)
        try:
            code_map = download_corp_code_map(sess)
        except (RuntimeError, requests.RequestException,
                zipfile.BadZipFile) as e:
            print(f"ERROR: corp_code download failed — {e}", file=sys.stderr)
            sys.exit(1)
        save_corp_code_cache(code_map)
        print(f"  downloaded {len(code_map):,} listed corp_code entries "
              f"(cached for {CORP_CACHE_MAX_AGE_DAYS}d)", flush=True)

    tickers = load_tickers()
    names = dict(tickers)
    pairs = [(t, code_map[t]) for t, _ in tickers if t in code_map]
    corp_to_ticker = {cc: t for t, cc in pairs}
    print(f"Loaded {len(tickers):,} tickers · {len(pairs):,} with corp_code",
          flush=True)

    today = datetime.now(KST)
    # The newest fiscal year worth querying. Before April the current year
    # has no annual report yet, but its quarters do exist.
    years = [today.year - i for i in range(QUERY_YEARS)]
    print(f"Querying fiscal years {years} x {len(REPORTS)} report codes…",
          flush=True)

    # Filing dates first. This sweep is cheap (a few dozen pages) but in
    # the first full run it sat behind the heavy fetch, the budget was
    # already spent by the time it started, and it returned nothing at
    # all — losing the 실적 발표일 half of the feature.
    print("Fetching 정기공시 filing dates…", flush=True)
    filings, filing_pages = fetch_report_filings(sess)
    print(f"  {filing_pages} pages · {len(filings):,} tickers with a recent "
          f"periodic report", flush=True)

    by_corp = defaultdict(lambda: defaultdict(dict))
    collect_periods(sess, [cc for _t, cc in pairs], years, by_corp)

    results = {}
    for corp, periods in by_corp.items():
        ticker = corp_to_ticker.get(corp)
        if not ticker:
            continue
        entry = build_entry(names.get(ticker, ticker), corp, periods)
        if entry is None:
            continue
        f = filings.get(ticker)
        if f:
            entry["lastReport"] = f
        results[ticker] = entry

    # Never let a degraded run shrink the dataset: tickers this run could
    # not reach keep whatever they already had.
    merged = dict(prev_fin)
    merged.update(results)

    over_budget = _DIAGS["budget"] > 0
    api_failed = _DIAGS["fail"] > 0
    clean = not over_budget and not api_failed
    now_iso = datetime.now(KST).isoformat(timespec="seconds")

    with_quarters = sum(1 for e in merged.values() if e.get("quarters"))
    payload = {
        "asOf": now_iso,
        "unit": "억원",
        "queryYears": years,
        "count": len(tickers),
        "covered": len(merged),
        "withQuarters": with_quarters,
        "lastRun": {
            "at": now_iso,
            "fetched": len(results),
            "apiFailedCalls": _DIAGS["fail"],
            "budgetSkipped": _DIAGS["budget"],
            "elapsedSec": int(time.monotonic() - started),
            "complete": clean,
        },
        "financials": merged,
    }
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    tmp = OUTPUT_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, separators=(",", ":"))
    os.replace(tmp, OUTPUT_FILE)

    print(
        f"Wrote {OUTPUT_FILE} · covered={len(merged):,} "
        f"(this run {len(results):,}) withQuarters={with_quarters:,} "
        f"({'complete' if clean else 'PARTIAL'})",
        flush=True,
    )

    print("\nPeriod-span diagnostics (how 분기 수치를 유도했는지):", flush=True)
    for k in ("span_cumulative", "span_discrete", "missing_prev_quarter",
              "negative_derived_revenue"):
        print(f"  {k:<26} {_DIAGS[k]:,}", flush=True)
    if _DIAGS["negative_derived_revenue"] > _DIAGS["span_cumulative"]:
        print(
            "  WARN: more negative derivations than clean ones — DART may be "
            "returning three-month figures where we assumed year-to-date.",
            flush=True,
        )
    if sum(_API_FAILURES.values()):
        print(f"\nAPI failures: {dict(_API_FAILURES)}", flush=True)


if __name__ == "__main__":
    main()
