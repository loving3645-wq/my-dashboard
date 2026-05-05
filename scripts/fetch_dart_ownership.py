#!/usr/bin/env python3
"""Largest-shareholder block + minority shareholder ratios per ticker.

Hits two 사업보고서 주요정보 endpoints:
  - hyslrSttus : 최대주주 현황 (controlling block — 본인 + 특수관계인)
  - mrhlSttus  : 소액주주 현황

Tries the most recent annual report (사업보고서, reprt_code=11011); falls
back one year if the latest filing isn't published yet for a given issuer.
Output: data/ownership.json keyed by ticker.

The frontend (stock-deep-dive.html) replaces the static "지분 구조"
placeholder with these numbers and rebuilds the donut.
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
OUTPUT_FILE = os.path.join(ROOT, "data", "ownership.json")

DART_KEY = os.environ.get("OPENDART_KEY", "").strip()
CORP_CODE_URL = "https://opendart.fss.or.kr/api/corpCode.xml"
HYSLR_URL = "https://opendart.fss.or.kr/api/hyslrSttus.json"
MRHL_URL = "https://opendart.fss.or.kr/api/mrhlSttus.json"

REPRT_ANNUAL = "11011"

WORKERS = 8
TIMEOUT = 15
RETRY_COUNT = 2


def latest_fiscal_year():
    today = datetime.now(KST)
    return today.year - 1 if today.month >= 4 else today.year - 2


def make_session():
    s = requests.Session()
    s.headers.update({"User-Agent": "my-dashboard/1.0 (ownership cron)"})
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


def _to_float(s):
    if s is None:
        return None
    s = str(s).replace(",", "").replace("%", "").strip()
    if not s or s == "-":
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _to_int(s):
    if s is None:
        return None
    s = str(s).replace(",", "").strip()
    if not s or s == "-":
        return None
    try:
        return int(float(s))
    except ValueError:
        return None


def _is_common_stock(stock_knd):
    """보통주 only — preferred and other classes get filtered out so the
    pie is a like-for-like cap-table breakdown."""
    if not stock_knd:
        return True  # filers that omit the field generally mean 보통주
    s = stock_knd.strip()
    return "보통" in s or s.lower() in ("common", "ordinary")


def _call_dart(sess, url, params):
    for attempt in range(RETRY_COUNT + 1):
        try:
            r = sess.get(url, params=params, timeout=TIMEOUT)
            if r.status_code != 200:
                return None
            body = r.json()
            status = body.get("status")
            if status == "013":
                return []
            if status != "000":
                if status in ("100", "101", "800", "900"):
                    time.sleep(0.5 * (attempt + 1))
                    continue
                return None
            return body.get("list") or []
        except (requests.RequestException, ValueError):
            time.sleep(0.5 * (attempt + 1))
            continue
    return None


def fetch_major_block(sess, corp_code, year):
    rows = _call_dart(sess, HYSLR_URL, {
        "crtfc_key": DART_KEY,
        "corp_code": corp_code,
        "bsns_year": str(year),
        "reprt_code": REPRT_ANNUAL,
    })
    if not rows:
        return None
    items = []
    total_ratio = 0.0
    total_shares = 0
    seen_keys = set()
    for r in rows:
        if not _is_common_stock(r.get("stock_knd")):
            continue
        ratio = _to_float(r.get("trmend_posesn_stock_qota_rt"))
        shares = _to_int(r.get("trmend_posesn_stock_co"))
        nm = (r.get("nm") or "").strip()
        relate = (r.get("relate") or "").strip()
        # Some filers append a "계" (subtotal) row — skip when nm has 계/합계
        # and relate is empty, to avoid double counting.
        if not nm or nm in ("계", "합계", "소계"):
            continue
        key = (nm, relate)
        if key in seen_keys:
            continue
        seen_keys.add(key)
        items.append({
            "name": nm,
            "relation": relate or None,
            "ratio": ratio,
            "shares": shares,
        })
        if ratio is not None:
            total_ratio += ratio
        if shares is not None:
            total_shares += shares
    if not items:
        return None
    items.sort(key=lambda x: x["ratio"] or 0, reverse=True)
    return {
        "totalRatio": round(total_ratio, 2) if total_ratio else None,
        "totalShares": total_shares or None,
        "items": items[:10],
    }


def fetch_minority_block(sess, corp_code, year):
    rows = _call_dart(sess, MRHL_URL, {
        "crtfc_key": DART_KEY,
        "corp_code": corp_code,
        "bsns_year": str(year),
        "reprt_code": REPRT_ANNUAL,
    })
    if not rows:
        return None
    # Most filers report a single 소액주주 row. Some break out per stock_knd.
    best = None
    for r in rows:
        if not _is_common_stock(r.get("stock_knd")):
            continue
        se = (r.get("se") or "").strip()
        if "소액" not in se:
            continue
        rate = _to_float(r.get("hold_stock_rate"))
        cnt = _to_int(r.get("shrholdr_co"))
        if rate is None:
            continue
        if best is None or (rate > (best.get("ratio") or 0)):
            best = {"ratio": rate, "shareholderCount": cnt}
    return best


def fetch_one(sess, ticker, name, corp_code, base_year):
    for year_offset in (0, 1):
        year = base_year - year_offset
        major = fetch_major_block(sess, corp_code, year)
        minority = fetch_minority_block(sess, corp_code, year)
        if major or minority:
            return {
                "name": name,
                "corpCode": corp_code,
                "fiscalYear": year,
                "majorBlock": major,
                "minorityBlock": minority,
            }
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
    base_year = latest_fiscal_year()
    print(f"Loaded {len(tickers):,} tickers · base year {base_year}", flush=True)

    results = {}
    skipped_no_corp = 0

    def task(ticker, name):
        cc = code_map.get(ticker)
        if not cc:
            return ticker, None, "no_corp"
        entry = fetch_one(sess, ticker, name, cc, base_year)
        return ticker, entry, None

    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futures = {ex.submit(task, t, n): t for t, n in tickers}
        done = 0
        total = len(futures)
        for fut in as_completed(futures):
            done += 1
            ticker, entry, reason = fut.result()
            if entry is not None:
                results[ticker] = entry
            elif reason == "no_corp":
                skipped_no_corp += 1
            if done % 200 == 0:
                print(
                    f"  {done}/{total} · ok={len(results)} "
                    f"no_corp={skipped_no_corp}",
                    flush=True,
                )

    payload = {
        "asOf": datetime.now(KST).isoformat(timespec="seconds"),
        "fiscalYearBase": base_year,
        "count": len(results),
        "ownership": results,
    }
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, separators=(",", ":"))

    print(f"Wrote {OUTPUT_FILE} · ok={len(results)}", flush=True)


if __name__ == "__main__":
    main()
