#!/usr/bin/env python3
"""Daily aggregator for Korean securities-firm research reports.

Naver migrated its research section to a JSON-backed SPA: the old
finance.naver.com/research/company_list.naver pages now return an empty
React shell, so this fetches the new m.stock.naver.com research APIs instead
and emits data/research-reports.json with the latest ~14 days of reports.

Categories:
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

# New Naver research JSON APIs (back the m.stock.naver.com research pages).
LIST_API = {
    "company": "https://m.stock.naver.com/api/research/company",
    "industry": "https://m.stock.naver.com/api/research/industry",
}
DETAIL_API = {
    "company": "https://m.stock.naver.com/api/research/company/{}",
    "industry": "https://m.stock.naver.com/api/research/industry/{}",
}
# Human-readable report page (for the '상세' link in the UI).
WEB_URL = {
    "company": "https://m.stock.naver.com/research/company/{}",
    "industry": "https://m.stock.naver.com/research/industry/{}",
}

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUTPUT_FILE = os.path.join(ROOT, "data", "research-reports.json")

USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0 Safari/537.36"
)

# 30 per page × 5 pages ≈ 150 reports per category, covers 5-7 trading days.
PAGES_PER_CATEGORY = 5
PAGE_SIZE = 30
WINDOW_DAYS = 14
DETAIL_WORKERS = 16

TAG_RE = re.compile(r"<[^>]+>")
# Target price / investment opinion are no longer structured fields — parse
# them best-effort from the report body HTML. Missing values stay None.
TARGET_RE = re.compile(r"목표\s*주?가[^\d]{0,12}([\d,]{4,})")
OPINION_RE = re.compile(
    r"투자\s*의견[^A-Za-z가-힣]{0,6}"
    r"([A-Za-z][A-Za-z.\s]{1,12}|매수|매도|중립|보유|비중\s*확대|비중\s*축소|강력매수)"
)


def make_session():
    s = requests.Session()
    s.headers.update({
        "User-Agent": USER_AGENT,
        "Accept": "application/json, text/plain, */*",
        "Referer": "https://m.stock.naver.com/",
        "Accept-Language": "ko-KR,ko;q=0.9,en;q=0.8",
    })
    return s


def strip_html(s):
    return TAG_RE.sub(" ", s or "").strip()


def normalize_date(s):
    s = (s or "").strip()
    # Already ISO (new API gives '2026-11-25')
    if re.match(r"\d{4}-\d{2}-\d{2}", s):
        return s[:10]
    # Legacy '26.05.04'
    m = re.match(r"(\d{2})\.(\d{2})\.(\d{2})", s)
    if m:
        yy, mm, dd = m.groups()
        yyyy = ("19" if int(yy) >= 70 else "20") + yy
        return f"{yyyy}-{mm}-{dd}"
    return s


def list_item_to_report(item, kind):
    """Map one JSON list item to our report dict."""
    rid = item.get("researchId")
    name = item.get("itemName") or item.get("category") or ""
    title = item.get("title") or ""
    house = item.get("brokerName") or ""
    if not title or not house or not rid:
        return None
    return {
        "type": kind,
        "date": normalize_date(item.get("writeDate")),
        "ticker": item.get("itemCode") if kind == "company" else None,
        "stockName": html_lib.unescape(name),
        "title": html_lib.unescape(title),
        "house": html_lib.unescape(house),
        "pdfUrl": None,            # filled from detail (attachUrl)
        "detailUrl": WEB_URL[kind].format(rid),
        "researchId": rid,
    }


def scrape_category(session, kind):
    """Fetch PAGES_PER_CATEGORY pages of one category from the JSON API."""
    url = LIST_API[kind]
    out = []
    for page in range(1, PAGES_PER_CATEGORY + 1):
        try:
            r = session.get(url, params={"page": page, "pageSize": PAGE_SIZE}, timeout=15)
            r.raise_for_status()
            items = r.json()
        except Exception as e:
            print(f"  page {page} fetch error: {e}", file=sys.stderr)
            continue
        if not isinstance(items, list):
            continue
        for it in items:
            rep = list_item_to_report(it, kind)
            if rep:
                out.append(rep)
    return out


def parse_target_price(s):
    """'160,000' / '160,000원' → int, or None."""
    if not s:
        return None
    cleaned = re.sub(r"[^\d]", "", s)
    if len(cleaned) < 4:
        return None
    try:
        return int(cleaned)
    except ValueError:
        return None


_OPINION_MAP = [
    ("강력매수", "Strong Buy"), ("매수", "Buy"), ("비중확대", "Overweight"),
    ("중립", "Hold"), ("보유", "Hold"), ("비중축소", "Underweight"),
    ("매도", "Sell"),
]


def normalize_opinion(s):
    if not s:
        return None
    s = s.strip()
    for ko, en in _OPINION_MAP:
        if ko in s.replace(" ", ""):
            return en
    low = s.lower()
    for en in ("strong buy", "buy", "overweight", "hold", "neutral",
               "marketperform", "market perform", "underweight", "reduce", "sell"):
        if en.replace(" ", "") in low.replace(" ", ""):
            return en.title()
    return None


def extract_summary(html_text):
    """Longest Korean-heavy text block from the report body (analyst's own
    writing), clipped to ~600 chars. No LLM."""
    if not html_text:
        return None
    # The new API body uses <br> between sentences — turn into spaces first.
    txt = re.sub(r"<br\s*/?>", " ", html_text, flags=re.I)
    chunks = re.findall(r">([^<>]{40,})<", txt) or [strip_html(txt)]
    best, best_score = "", 0
    for c in chunks:
        c = c.strip()
        if len(c) < 80:
            continue
        if any(tok in c for tok in ("function", "var ", "</", "==", "()=>",
                                     "/*", "window.", "document.")):
            continue
        korean = len(re.findall(r"[가-힣]", c))
        density = korean / len(c)
        if density < 0.3:
            continue
        score = len(c) * density
        if score > best_score:
            best_score = score
            best = c
    if not best:
        return None
    best = re.sub(r"\s+", " ", best).strip()
    if len(best) > 600:
        best = best[:600].rsplit(" ", 1)[0] + "…"
    return best


def fetch_detail(session, kind, rid):
    """Fetch one report's detail JSON → pdf + target price + opinion + summary."""
    r = session.get(DETAIL_API[kind].format(rid), timeout=15)
    r.raise_for_status()
    content = (r.json() or {}).get("researchContent") or {}
    body = content.get("content") or ""
    tm = TARGET_RE.search(body)
    om = OPINION_RE.search(body)
    return {
        "pdfUrl": content.get("attachUrl"),
        "targetPrice": parse_target_price(tm.group(1)) if tm else None,
        "opinion": normalize_opinion(om.group(1)) if om else None,
        "summary": extract_summary(body),
    }


def enrich_with_detail(session, reports):
    """Fetch detail for every report (company + industry) to fill pdf/summary,
    plus target price + opinion for company reports."""
    targets = [r for r in reports if r.get("researchId")]
    print(f"  enriching {len(targets)} reports with detail data...", flush=True)
    completed = 0
    errors = 0
    with ThreadPoolExecutor(max_workers=DETAIL_WORKERS) as ex:
        futures = {
            ex.submit(fetch_detail, session, r["type"], r["researchId"]): r
            for r in targets
        }
        for fut in as_completed(futures):
            rep = futures[fut]
            completed += 1
            try:
                info = fut.result()
                # Industry reports have no ticker-level target/opinion.
                if rep.get("type") != "company":
                    info.pop("targetPrice", None)
                    info.pop("opinion", None)
                rep.update({k: v for k, v in info.items() if v is not None})
            except Exception:
                errors += 1
            if completed % 50 == 0:
                print(f"    detail {completed}/{len(targets)} (err={errors})", flush=True)
    print(f"  detail done ({errors} errors)", flush=True)


NEW_COVERAGE_MARKERS = (
    "커버리지 개시", "신규 커버리지", "신규 커버", "커버리지 시작",
    "Initiating", "Initiate", "First Look", "최초 커버",
)


def is_initiating_coverage(title):
    if not title:
        return False
    t = title.lower()
    return any(m.lower() in t for m in NEW_COVERAGE_MARKERS)


def tag_target_changes(reports):
    """Tag each company report's target vs the same house's prior report on the
    same stock within the window: 'up'/'flat'/'down'/'new'/None."""
    company = [r for r in reports if r.get("type") == "company"]
    company.sort(key=lambda r: (r.get("ticker", ""), r.get("house", ""), r.get("date", "")))
    last_target = {}
    for r in company:
        key = (r.get("ticker"), r.get("house"))
        cur = r.get("targetPrice")
        prev = last_target.get(key)
        if cur is None:
            r["targetChange"] = None
            r["prevTargetPrice"] = None
        elif prev is None:
            r["targetChange"] = "new" if is_initiating_coverage(r.get("title")) else None
            r["prevTargetPrice"] = None
        elif cur > prev:
            r["targetChange"] = "up"; r["prevTargetPrice"] = prev
        elif cur < prev:
            r["targetChange"] = "down"; r["prevTargetPrice"] = prev
        else:
            r["targetChange"] = "flat"; r["prevTargetPrice"] = prev
        if cur is not None:
            last_target[key] = cur


def main():
    session = make_session()

    all_reports = []
    for kind in LIST_API:
        print(f"Scraping {kind}...", flush=True)
        rows = scrape_category(session, kind)
        print(f"  {len(rows)} {kind} reports", flush=True)
        all_reports.extend(rows)

    enrich_with_detail(session, all_reports)

    # Deduplicate by detailUrl (or pdfUrl)
    seen = set()
    deduped = []
    for r in all_reports:
        key = r.get("detailUrl") or r.get("pdfUrl") or (r.get("date", "") + r.get("title", ""))
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
            fresh.append(r)

    tag_target_changes(fresh)
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
        print(f"WARN: only {len(fresh)} reports — Naver API may have changed.",
              file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
