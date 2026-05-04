#!/usr/bin/env python3
"""38커뮤니케이션에서 공모주 청약/상장 일정 스크랩 → data/ipo-calendar.json.

소스: http://www.38.co.kr — 한국 IPO 정보 표준 사이트.
- 공모청약 일정: /html/fund/index.htm?o=k
- 신규상장 (청약 종료, 상장 대기): /html/fund/index.htm?o=v

각 행에서 종목명·청약기간·환불일·상장일·공모가·주간사 등을 best-effort로
파싱한다. 38.co.kr HTML 구조가 종종 바뀌므로 보수적으로:
  - <tr> 별로 셀 텍스트 추출
  - 날짜 패턴(YYYY.MM.DD 또는 YYYY-MM-DD) n개 추출
  - 회사명 셀에서 anchor href의 detail 번호 추출
실패해도 스크립트는 빈 배열로 끝나며 cron이 깨지지 않도록 함.
"""

import json
import os
import re
import sys
from datetime import datetime, timedelta, timezone

import requests

KST = timezone(timedelta(hours=9))
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUTPUT_FILE = os.path.join(ROOT, "data", "ipo-calendar.json")

USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
)

BASE = "http://www.38.co.kr"
URL_UPCOMING = f"{BASE}/html/fund/index.htm?o=k"    # 공모청약 일정
URL_SCHEDULED = f"{BASE}/html/fund/index.htm?o=nw"  # 신규상장종목 (상장일 포함)

ENCODING = "euc-kr"

DATE_RE = re.compile(r"(\d{4})[./-](\d{2})[./-](\d{2})")
SHORT_DATE_RE = re.compile(r"(?<![\d.])(\d{2})[./-](\d{2})(?![\d.])")
NUM_RE = re.compile(r"[\d,]+")
PRICE_BAND_RE = re.compile(r"^([\d,]+)\s*[~\-]\s*([\d,]+)$")


def fetch_html(url, timeout=20):
    r = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=timeout)
    r.raise_for_status()
    r.encoding = ENCODING
    return r.text


def strip_tags(s):
    s = re.sub(r"<br\s*/?>", " ", s, flags=re.I)
    s = re.sub(r"<[^>]+>", "", s)
    s = re.sub(r"&nbsp;", " ", s)
    s = re.sub(r"&amp;", "&", s)
    return re.sub(r"\s+", " ", s).strip()


def normalize_date(year, month, day):
    return f"{year}-{month}-{day}"


def parse_dates(text):
    return [normalize_date(*m) for m in DATE_RE.findall(text or "")]


def parse_period(text):
    """청약기간 셀 — 'YYYY.MM.DD~MM.DD' 또는 'YYYY.MM.DD~YYYY.MM.DD' 모두 처리.
    반환: (start_iso, end_iso). 못 파싱하면 (None, None)."""
    if not text:
        return (None, None)
    parts = re.split(r"\s*~\s*", text, maxsplit=1)
    full = DATE_RE.findall(parts[0])
    if not full:
        return (None, None)
    start = normalize_date(*full[0])
    if len(parts) < 2:
        return (start, start)
    tail = parts[1]
    full_tail = DATE_RE.findall(tail)
    if full_tail:
        return (start, normalize_date(*full_tail[0]))
    short = SHORT_DATE_RE.search(tail)
    if short:
        m, d = short.group(1), short.group(2)
        start_year = int(full[0][0])
        end_year = start_year + (1 if int(m) < int(full[0][1]) else 0)
        return (start, normalize_date(str(end_year), m, d))
    return (start, start)


def parse_rows(html):
    """모든 <tr>을 셀 텍스트 + anchor href 리스트로 변환."""
    rows = []
    for tr_match in re.finditer(r"<tr[^>]*>(.*?)</tr>", html, re.S | re.I):
        tr_html = tr_match.group(1)
        cells = []
        for td_match in re.finditer(r"<td[^>]*>(.*?)</td>", tr_html, re.S | re.I):
            td_html = td_match.group(1)
            href = None
            a = re.search(r"href=[\"']([^\"']+)[\"']", td_html, re.I)
            if a:
                href = a.group(1)
            cells.append({"text": strip_tags(td_html), "href": href})
        if cells:
            rows.append(cells)
    return rows


def extract_detail_no(href):
    """38.co.kr 종목 상세 URL 에서 no 파라미터 추출 (있을 때만)."""
    if not href:
        return None
    m = re.search(r"no=(\d+)", href)
    return m.group(1) if m else None


def parse_upcoming(html):
    """38.co.kr 공모청약 일정.
    실측 컬럼: [종목명, 청약일(YYYY.MM.DD~MM.DD), 확정공모가, 공모가밴드, 경쟁률, 주간사, 의무확약]"""
    out = []
    today = datetime.now(KST).date()
    cutoff = today - timedelta(days=2)
    for cells in parse_rows(html):
        if len(cells) < 5:
            continue
        name = cells[0]["text"]
        if not name or len(name) > 40 or any(k in name for k in ("종목명", "회사", "공모가")):
            continue
        if not cells[0]["href"]:
            continue
        sub_start, sub_end = parse_period(cells[1]["text"]) if len(cells) > 1 else (None, None)
        if not sub_start:
            continue
        try:
            if datetime.strptime(sub_start, "%Y-%m-%d").date() < cutoff and \
               (not sub_end or datetime.strptime(sub_end, "%Y-%m-%d").date() < cutoff):
                continue
        except ValueError:
            continue
        # 확정공모가 = cell[2], 밴드 = cell[3]
        ipo_price = None
        if len(cells) > 2:
            t = cells[2]["text"].replace(",", "")
            if t.isdigit():
                ipo_price = int(t)
        ipo_band = None
        if len(cells) > 3:
            m = PRICE_BAND_RE.match(cells[3]["text"].strip())
            if m:
                ipo_band = f"{m.group(1)}~{m.group(2)}"
        underwriter = None
        for c in cells[4:7] if len(cells) >= 7 else cells[3:]:
            t = c["text"]
            if ("증권" in t or "투자" in t) and len(t) < 50:
                underwriter = t
                break
        out.append({
            "name": name,
            "detailNo": extract_detail_no(cells[0]["href"]),
            "subscriptionStart": sub_start,
            "subscriptionEnd": sub_end,
            "ipoPrice": ipo_price,
            "ipoPriceBand": ipo_band,
            "underwriter": underwriter,
            "listingDate": None,  # 이 페이지엔 없음 — scheduled 페이지에서 join
        })
    return out


def parse_scheduled(html):
    """38.co.kr 신규상장종목 (o=nw).
    실측 컬럼: [종목명, 상장일(YYYY/MM/DD), ..., 공모가, ..., 상태(예정/상장)]
    여기선 상장일과 공모가만 안전하게 추출하고, 다른 정보는 upcoming과 join."""
    out = []
    today = datetime.now(KST).date()
    cutoff = today - timedelta(days=2)
    for cells in parse_rows(html):
        if len(cells) < 4:
            continue
        name = cells[0]["text"]
        if not name or len(name) > 40 or any(k in name for k in ("종목명", "회사")):
            continue
        if not cells[0]["href"]:
            continue
        # cell[1]에 상장일
        dates = parse_dates(cells[1]["text"]) if len(cells) > 1 else []
        if not dates:
            continue
        listing = dates[0]
        try:
            if datetime.strptime(listing, "%Y-%m-%d").date() < cutoff:
                continue
        except ValueError:
            continue
        # 공모가는 보통 cell[4] — 숫자만 들어있는 셀 추출
        ipo_price = None
        for c in cells[2:6]:
            t = c["text"].replace(",", "").replace("\xa0", "").strip()
            if t.isdigit() and 100 <= int(t) <= 10_000_000:
                ipo_price = int(t)
                break
        out.append({
            "name": name,
            "detailNo": extract_detail_no(cells[0]["href"]),
            "listingDate": listing,
            "ipoPrice": ipo_price,
        })
    return out


def main():
    print("Fetching 38.co.kr 공모청약 일정...", flush=True)
    upcoming = []
    scheduled = []
    errors = []
    try:
        html = fetch_html(URL_UPCOMING)
        upcoming = parse_upcoming(html)
        print(f"  upcoming: {len(upcoming)} rows", flush=True)
    except Exception as e:
        errors.append(f"upcoming: {e}")
        print(f"  WARN upcoming fetch failed: {e}", file=sys.stderr, flush=True)

    try:
        html = fetch_html(URL_SCHEDULED)
        scheduled = parse_scheduled(html)
        print(f"  scheduled: {len(scheduled)} rows", flush=True)
    except Exception as e:
        errors.append(f"scheduled: {e}")
        print(f"  WARN scheduled fetch failed: {e}", file=sys.stderr, flush=True)

    # 두 페이지를 detailNo 또는 name으로 join — upcoming(청약일/공모가) + scheduled(상장일)
    by_key = {}
    for r in upcoming:
        key = r.get("detailNo") or r.get("name")
        by_key[key] = dict(r)
    for r in scheduled:
        key = r.get("detailNo") or r.get("name")
        if key in by_key:
            # listingDate / ipoPrice 보강 (없는 필드만)
            if not by_key[key].get("listingDate"):
                by_key[key]["listingDate"] = r.get("listingDate")
            if not by_key[key].get("ipoPrice"):
                by_key[key]["ipoPrice"] = r.get("ipoPrice")
        else:
            by_key[key] = dict(r)
    merged = list(by_key.values())

    def sortkey(r):
        return r.get("subscriptionStart") or r.get("listingDate") or "9999-12-31"
    merged.sort(key=sortkey)

    output = {
        "generatedAt": datetime.now(KST).isoformat(timespec="seconds"),
        "source": "38.co.kr",
        "count": len(merged),
        "upcoming": merged,
        "errors": errors,
    }
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, separators=(",", ":"))
    size_kb = os.path.getsize(OUTPUT_FILE) / 1024
    print(f"Wrote {OUTPUT_FILE} ({size_kb:.1f} KB, {len(merged)} entries, errors={len(errors)})")


if __name__ == "__main__":
    main()
