#!/usr/bin/env python3
"""Recent DART disclosures (last 30 days, top 10) per ticker.

Lightweight feed via OpenDART list.json — title + date + 접수번호 only.
No body parsing; the UI links each title to the DART viewer at
dart.fss.or.kr/dsaf001/main.do?rcpNo={rceptNo} so analysts can drill in.

Run daily after market close.
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
OUTPUT_FILE = os.path.join(ROOT, "data", "disclosures.json")

DART_KEY = os.environ.get("OPENDART_KEY", "").strip()
CORP_CODE_URL = "https://opendart.fss.or.kr/api/corpCode.xml"
LIST_URL = "https://opendart.fss.or.kr/api/list.json"

LOOKBACK_DAYS = 30
PER_TICKER_LIMIT = 10
PAGE_SIZE = 30  # one page is enough since we cap at 10 per ticker

WORKERS = 12
TIMEOUT = 15
RETRY_COUNT = 2

# Map pblntf_ty (top-level category) → short Korean label for the UI.
KIND_LABEL = {
    "A": "정기",
    "B": "발행",
    "C": "지분",
    "D": "주요",
    "E": "감사",
    "F": "펀드",
    "G": "유동화",
    "H": "거래소",
    "I": "공정위",
    "J": "기타",
}


def make_session():
    s = requests.Session()
    s.headers.update({"User-Agent": "my-dashboard/1.0 (disclosures cron)"})
    return s


def load_tickers():
    with open(TICKERS_FILE, encoding="utf-8") as f:
        text = f.read()
    out = []
    for m in re.finditer(r"'([0-9A-Z]{6})':\s*\{\s*name:\s*'([^']+)'", text):
        out.append((m.group(1), m.group(2)))
    return out


def download_corp_code_map(sess):
    r = sess.get(CORP_CODE_URL, params={"crtfc_key": DART_KEY}, timeout=30)
    r.raise_for_status()
    zf = zipfile.ZipFile(io.BytesIO(r.content))
    xml_bytes = zf.read(zf.namelist()[0])
    root = ET.fromstring(xml_bytes)
    mapping = {}
    for elt in root.findall("list"):
        sc = (elt.findtext("stock_code") or "").strip()
        cc = (elt.findtext("corp_code") or "").strip()
        if sc and cc:
            mapping[sc] = cc
    return mapping


def _to_iso_date(s):
    if not s:
        return None
    s = s.strip()
    m = re.match(r"^(\d{4})[\-./]?(\d{2})[\-./]?(\d{2})", s)
    if not m:
        return None
    return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"


def fetch_one(sess, corp_code, bgn_de, end_de):
    params = {
        "crtfc_key": DART_KEY,
        "corp_code": corp_code,
        "bgn_de": bgn_de,
        "end_de": end_de,
        "page_count": PAGE_SIZE,
        "page_no": 1,
    }
    for attempt in range(RETRY_COUNT + 1):
        try:
            r = sess.get(LIST_URL, params=params, timeout=TIMEOUT)
            if r.status_code != 200:
                return []
            body = r.json()
            status = body.get("status")
            if status == "013":
                return []
            if status != "000":
                if status in ("100", "101", "800", "900"):
                    time.sleep(0.5 * (attempt + 1))
                    continue
                return []
            rows = body.get("list") or []
            out = []
            for row in rows[:PER_TICKER_LIMIT]:
                ty = (row.get("pblntf_ty") or "").strip()
                out.append({
                    "date": _to_iso_date(row.get("rcept_dt") or ""),
                    "rceptNo": (row.get("rcept_no") or "").strip() or None,
                    "title": (row.get("report_nm") or "").strip(),
                    "filer": (row.get("flr_nm") or "").strip() or None,
                    "kind": KIND_LABEL.get(ty, ty or None),
                })
            return out
        except (requests.RequestException, ValueError):
            time.sleep(0.5 * (attempt + 1))
            continue
    return []


def main():
    if not DART_KEY:
        print("ERROR: OPENDART_KEY env var not set", file=sys.stderr)
        sys.exit(1)

    sess = make_session()
    print("Fetching DART corp_code master…", flush=True)
    code_map = download_corp_code_map(sess)
    print(f"  loaded {len(code_map):,} listed corp_code entries", flush=True)

    tickers = load_tickers()
    today = datetime.now(KST).date()
    bgn = (today - timedelta(days=LOOKBACK_DAYS)).strftime("%Y%m%d")
    end = today.strftime("%Y%m%d")
    print(f"Loaded {len(tickers):,} tickers · range {bgn} → {end}", flush=True)

    results = {}
    skipped_no_corp = 0
    total_disc = 0

    def task(ticker):
        cc = code_map.get(ticker)
        if not cc:
            return ticker, None, "no_corp"
        items = fetch_one(sess, cc, bgn, end)
        if not items:
            return ticker, None, None
        return ticker, items, None

    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futures = {ex.submit(task, t): t for t, _ in tickers}
        done = 0
        total = len(futures)
        for fut in as_completed(futures):
            done += 1
            ticker, items, reason = fut.result()
            if items:
                results[ticker] = items
                total_disc += len(items)
            elif reason == "no_corp":
                skipped_no_corp += 1
            if done % 300 == 0:
                print(
                    f"  {done}/{total} · with_disc={len(results)} "
                    f"total={total_disc}",
                    flush=True,
                )

    payload = {
        "asOf": datetime.now(KST).isoformat(timespec="seconds"),
        "rangeStart": bgn,
        "rangeEnd": end,
        "tickersWithDisclosures": len(results),
        "totalDisclosures": total_disc,
        "disclosures": results,
    }
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, separators=(",", ":"))

    print(
        f"Wrote {OUTPUT_FILE} · tickers={len(results)} disc={total_disc}",
        flush=True,
    )


if __name__ == "__main__":
    main()
