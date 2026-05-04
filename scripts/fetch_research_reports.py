#!/usr/bin/env python3
"""Daily aggregator for Korean securities-firm research reports.

Scrapes Naver Finance's research listing pages — the same listings
brokerages (한화/미래에셋/삼성증권 etc.) publish their daily reports to —
and emits data/research-reports.json with the latest ~14 days of reports.

Categories scraped:
  - company  (개별 종목 분석 리포트)
  - industry (업종 리포트)

Run daily via GitHub Actions cron after market close.
"""

import html as html_lib
import json
import os
import re
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

import requests

KST = timezone(timedelta(hours=9))

BASE = "https://finance.naver.com/research"
LISTS = {
    "company": f"{BASE}/company_list.naver",
    "industry": f"{BASE}/industry_list.naver",
}

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUTPUT_FILE = os.path.join(ROOT, "data", "research-reports.json")

USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0 Safari/537.36"
)

# How many pages to scrape per category. Naver shows ~30 per page.
# 5 pages × 30 ≈ 150 reports per category, comfortably covers 5-7 trading days.
PAGES_PER_CATEGORY = 5

# Window for keeping reports in the JSON output (older are dropped).
WINDOW_DAYS = 14

# Detail-page enrichment for company reports. We fetch each report's detail
# page to extract target price + investment opinion (not present on the list
# page). 16 workers comfortably handles ~150 reports in <15s on Actions.
DETAIL_WORKERS = 16

# Regex pre-compiled for speed
ROW_RE = re.compile(r"<tr[^>]*>(.+?)</tr>", re.S)
TD_RE = re.compile(r"<td[^>]*>(.+?)</td>", re.S)
TABLE_RE = re.compile(r'<table[^>]*class="type_1"[^>]*>(.+?)</table>', re.S)
HREF_CODE_RE = re.compile(r"code=(\d{6})")
HREF_NID_RE = re.compile(r"nid=(\d+)")
PDF_RE = re.compile(r'href="([^"]+\.pdf)"')
TAG_RE = re.compile(r"<[^>]+>")
# Detail page extractors. Naver renders these inside the .view_info block:
#   목표가 <em class="money"><strong>160,000</strong></em>
#   투자의견 <em class="coment">Buy</em>
TARGET_RE = re.compile(
    r"목표가\s*<em[^>]*class=\"money\"[^>]*>\s*<strong[^>]*>([^<]+)</strong>",
    re.S,
)
OPINION_RE = re.compile(
    r"투자의견\s*<em[^>]*class=\"coment\"[^>]*>([^<]+)</em>",
    re.S,
)


def make_session():
    s = requests.Session()
    s.headers.update({
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml",
        "Referer": "https://finance.naver.com/",
        "Accept-Language": "ko-KR,ko;q=0.9,en;q=0.8",
    })
    return s


def fetch_page(session, url, page):
    """Fetch one Naver research list page. Returns decoded HTML text."""
    r = session.get(url, params={"page": page}, timeout=15)
    r.raise_for_status()
    return r.content.decode("euc-kr", errors="replace")


def strip_html(s):
    return TAG_RE.sub("", s).strip()


def normalize_date(s):
    # Naver shows '26.05.04' → convert to '2026-05-04'
    s = s.strip()
    m = re.match(r"(\d{2})\.(\d{2})\.(\d{2})", s)
    if not m:
        return s
    yy, mm, dd = m.groups()
    yyyy = ("19" if int(yy) >= 70 else "20") + yy
    return f"{yyyy}-{mm}-{dd}"


def parse_company_row(row_html):
    """Parse one row from company_list.naver. Returns dict or None."""
    # Stock name + code (1st td has anchor with code= URL)
    code_match = HREF_CODE_RE.search(row_html)
    if not code_match:
        return None
    code = code_match.group(1)

    # Cells (skip empty leading/trailing whitespace cells)
    cells = TD_RE.findall(row_html)
    if len(cells) < 5:
        return None

    stock_name = strip_html(cells[0])
    title = strip_html(cells[1])
    house = strip_html(cells[2])

    # PDF
    pdf_match = PDF_RE.search(cells[3] if len(cells) > 3 else "")
    pdf_url = pdf_match.group(1) if pdf_match else None

    # Date is usually 5th cell (index 4)
    date = normalize_date(strip_html(cells[4]) if len(cells) > 4 else "")

    # Detail page nid
    nid_match = HREF_NID_RE.search(row_html)
    detail_url = (
        f"{BASE}/company_read.naver?nid={nid_match.group(1)}"
        if nid_match else None
    )

    if not stock_name or not title or not house:
        return None

    return {
        "type": "company",
        "date": date,
        "ticker": code,
        "stockName": html_lib.unescape(stock_name),
        "title": html_lib.unescape(title),
        "house": html_lib.unescape(house),
        "pdfUrl": pdf_url,
        "detailUrl": detail_url,
    }


def parse_industry_row(row_html):
    """Parse one row from industry_list.naver. Same shape minus ticker."""
    cells = TD_RE.findall(row_html)
    if len(cells) < 4:
        return None
    industry = strip_html(cells[0])
    title = strip_html(cells[1])
    house = strip_html(cells[2])
    pdf_match = PDF_RE.search(cells[3] if len(cells) > 3 else "")
    pdf_url = pdf_match.group(1) if pdf_match else None
    date = normalize_date(strip_html(cells[4]) if len(cells) > 4 else "")
    nid_match = HREF_NID_RE.search(row_html)
    detail_url = (
        f"{BASE}/industry_read.naver?nid={nid_match.group(1)}"
        if nid_match else None
    )
    if not title or not house:
        return None
    return {
        "type": "industry",
        "date": date,
        "ticker": None,
        "stockName": html_lib.unescape(industry),  # 업종명을 stockName 자리에 둠
        "title": html_lib.unescape(title),
        "house": html_lib.unescape(house),
        "pdfUrl": pdf_url,
        "detailUrl": detail_url,
    }


def scrape_category(session, kind):
    """Scrape PAGES_PER_CATEGORY pages of one category. Returns list of dicts."""
    url = LISTS[kind]
    parser = parse_company_row if kind == "company" else parse_industry_row
    out = []
    for page in range(1, PAGES_PER_CATEGORY + 1):
        try:
            html = fetch_page(session, url, page)
        except Exception as e:
            print(f"  page {page} fetch error: {e}", file=sys.stderr)
            continue
        m = TABLE_RE.search(html)
        if not m:
            continue
        for row in ROW_RE.findall(m.group(1)):
            row_data = parser(row)
            if row_data:
                out.append(row_data)
    return out


def parse_target_price(s):
    """'160,000' / '160,000원' / '1,234.5' → int. Returns None if unparseable."""
    if not s:
        return None
    cleaned = re.sub(r"[^\d.]", "", s)
    if not cleaned:
        return None
    try:
        return int(float(cleaned))
    except ValueError:
        return None


def fetch_detail(session, detail_url):
    """Fetch one report detail page and extract target price + opinion."""
    r = session.get(detail_url, timeout=15)
    r.raise_for_status()
    text = r.content.decode("euc-kr", errors="replace")
    target_match = TARGET_RE.search(text)
    opinion_match = OPINION_RE.search(text)
    return {
        "targetPrice": parse_target_price(target_match.group(1)) if target_match else None,
        "opinion": html_lib.unescape(opinion_match.group(1).strip()) if opinion_match else None,
    }


def enrich_with_detail(session, reports):
    """Fetch detail page for each company report in parallel and attach
    target price + opinion. Industry reports skip enrichment (no targets)."""
    targets = [r for r in reports if r.get("type") == "company" and r.get("detailUrl")]
    print(f"  enriching {len(targets)} company reports with detail data...", flush=True)
    completed = 0
    errors = 0
    by_url = {r["detailUrl"]: r for r in targets}
    with ThreadPoolExecutor(max_workers=DETAIL_WORKERS) as ex:
        futures = {ex.submit(fetch_detail, session, url): url for url in by_url}
        for fut in as_completed(futures):
            url = futures[fut]
            completed += 1
            try:
                info = fut.result()
                by_url[url].update(info)
            except Exception as e:
                errors += 1
                # Don't print every error; keep log tidy
            if completed % 50 == 0:
                print(
                    f"    detail {completed}/{len(by_url)} (err={errors})",
                    flush=True,
                )
    print(f"  detail done ({errors} errors)", flush=True)


# True "initiating coverage" markers — only tag as 'new' when the title
# explicitly says so. Without this, "no prior data in our 14-day window"
# was being misread as "first ever coverage" which is wrong for any
# blue-chip already covered for years (한화투자증권 vs LG에너지솔루션 etc.).
NEW_COVERAGE_MARKERS = (
    "커버리지 개시",
    "신규 커버리지",
    "신규 커버",
    "커버리지 시작",
    "Initiating",
    "Initiate",
    "First Look",
    "최초 커버",
)


def is_initiating_coverage(title):
    if not title:
        return False
    t = title.lower()
    return any(m.lower() in t for m in NEW_COVERAGE_MARKERS)


def tag_target_changes(reports):
    """For each company report, tag whether the target price was raised /
    held / lowered vs the same securities firm's previous report on the same
    stock *within our scrape window*.

    Sets r['targetChange'] in {'up', 'flat', 'down', 'new', None}:
      - 'up' / 'flat' / 'down': we have a prior report from the same house
        on the same stock — direct comparison.
      - 'new': only when the report title contains an explicit "신규 커버리지
        / Initiating Coverage" marker. We do NOT label first-seen-in-window
        as 'new' because most blue chips are continuously covered for years
        and our 14-day window simply lacks the older baseline.
      - None: target price unknown OR we don't have a baseline yet
        (most reports on first scrape — settles as cron accumulates).
    """
    # Sort oldest-first within each (ticker, house) group
    company = [r for r in reports if r.get("type") == "company"]
    company.sort(key=lambda r: (r.get("ticker", ""), r.get("house", ""), r.get("date", "")))

    last_target = {}  # (ticker, house) -> last seen targetPrice
    for r in company:
        key = (r.get("ticker"), r.get("house"))
        cur = r.get("targetPrice")
        prev = last_target.get(key)
        if cur is None:
            r["targetChange"] = None
            r["prevTargetPrice"] = None
        elif prev is None:
            # No baseline in window. Only call it 'new' if the title says so.
            r["targetChange"] = "new" if is_initiating_coverage(r.get("title")) else None
            r["prevTargetPrice"] = None
        elif cur > prev:
            r["targetChange"] = "up"
            r["prevTargetPrice"] = prev
        elif cur < prev:
            r["targetChange"] = "down"
            r["prevTargetPrice"] = prev
        else:
            r["targetChange"] = "flat"
            r["prevTargetPrice"] = prev
        if cur is not None:
            last_target[key] = cur


def main():
    session = make_session()

    all_reports = []
    for kind in LISTS:
        print(f"Scraping {kind}...", flush=True)
        rows = scrape_category(session, kind)
        print(f"  {len(rows)} {kind} reports", flush=True)
        all_reports.extend(rows)

    # Enrich with detail-page target price + opinion (company reports only)
    enrich_with_detail(session, all_reports)

    # Deduplicate by detailUrl (or pdfUrl)
    seen = set()
    deduped = []
    for r in all_reports:
        key = r.get("detailUrl") or r.get("pdfUrl") or (r.get("date") + r.get("title", ""))
        if key in seen:
            continue
        seen.add(key)
        deduped.append(r)

    # Trim to window
    today = datetime.now(KST).date()
    cutoff = today - timedelta(days=WINDOW_DAYS)
    fresh = []
    for r in deduped:
        try:
            d = datetime.strptime(r["date"], "%Y-%m-%d").date()
            if d >= cutoff:
                fresh.append(r)
        except (ValueError, KeyError):
            # Keep entries with bad date format — better than dropping
            fresh.append(r)

    # Compute target-price change tags (up / flat / down / new) by comparing
    # against the same house's prior report on the same stock, within window.
    tag_target_changes(fresh)

    # Sort newest first
    fresh.sort(key=lambda r: (r.get("date", ""), r.get("type", "")), reverse=True)

    out = {
        "generatedAt": datetime.now(KST).isoformat(timespec="seconds"),
        "windowDays": WINDOW_DAYS,
        "count": len(fresh),
        "reports": fresh,
    }

    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, separators=(",", ":"))

    print(
        f"Wrote {len(fresh)} reports to {OUTPUT_FILE} "
        f"(window {WINDOW_DAYS}d, generated {out['generatedAt']})",
        flush=True,
    )

    if len(fresh) < 5:
        print(
            f"WARN: only {len(fresh)} reports — Naver layout may have changed.",
            file=sys.stderr,
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
