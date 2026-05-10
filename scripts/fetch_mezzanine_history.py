#!/usr/bin/env python3
"""Mezzanine (CB/BW/EB) issuance history per KOSPI/KOSDAQ ticker via OpenDART.

Hits three structured endpoints under 주요사항보고서:
  - cvbdIsDecsn  : 전환사채권 발행결정      (CB)
  - bdwtIsDecsn  : 신주인수권부사채권 발행결정 (BW)
  - exbdIsDecsn  : 교환사채권 발행결정      (EB)

DART parses the major-event reports for us — no document body parsing
needed. We normalize the response into a uniform issuance schema and
compute a simple "outstanding by maturity" estimate (sum of face amounts
where maturity is still in the future). Real outstanding requires
tracking conversion / put / call exercises and is out of scope for v1;
the UI labels the figure accordingly.

Output: data/mezzanine.json keyed by ticker.

Run weekly — issuance announcements aren't constant but new ones can
appear any business day for small caps.
"""

import io
import json
import os
import re
import sys
import threading
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, date, timedelta, timezone
from xml.etree import ElementTree as ET

import requests

KST = timezone(timedelta(hours=9))

# One sample raw row per endpoint, captured on first non-empty response.
# Dumped at the end of main() if any normalized field has < 5% fill rate
# (i.e. a likely DART key drift) so the actual response keys are visible
# in CI logs for the next round of fixing.
_SAMPLE_ROWS = {}
_SAMPLE_LOCK = threading.Lock()


def _record_sample(kind, row):
    with _SAMPLE_LOCK:
        _SAMPLE_ROWS.setdefault(kind, dict(row))

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TICKERS_FILE = os.path.join(ROOT, "tickers-full.js")
OUTPUT_FILE = os.path.join(ROOT, "data", "mezzanine.json")

DART_KEY = os.environ.get("OPENDART_KEY", "").strip()
CORP_CODE_URL = "https://opendart.fss.or.kr/api/corpCode.xml"
LIST_URL = "https://opendart.fss.or.kr/api/list.json"
DOCUMENT_URL = "https://opendart.fss.or.kr/api/document.xml"

# 7-year lookback covers virtually all live mezzanine — typical CB tenor
# is 3-5 years; even rare 7-year issues with year-2 puts will be picked up.
LOOKBACK_YEARS = 7

# Cap pagination per ticker for the action-event scan. 10 pages × 100 =
# 1000 disclosures over 7 years, more than enough for any Korean issuer.
LIST_MAX_PAGES = 10
LIST_PAGE_SIZE = 100

# Title-pattern → event type. 전환청구권행사 reduces principal but the
# AMOUNT isn't in the title — we'd need to fetch the disclosure body to
# get it. v2 only captures the event itself; v3 will parse bodies for
# accurate net outstanding.
ROUND_RE = re.compile(r"제\s*(\d+(?:[\-–]\d+)?)\s*회")

# Patterns for the "outstanding after exercise" amount in 전환청구권행사
# / 사채권조기상환 / 만기상환 disclosure bodies. Filers' phrasing varies;
# we try several. All capture an integer amount in 원. After XML tag
# stripping, table headers and cell values often appear adjacent.
OUTSTANDING_PATTERNS = [
    re.compile(
        r"(?:행사\s*후|전환\s*후|상환\s*후)\s*(?:미상환\s*)?(?:사채\s*)?"
        r"(?:잔액|잔여\s*사채\s*총액|액면\s*총액|총액|잔)\s*[:：\s]*"
        r"([\d,]+)\s*원?"
    ),
    re.compile(
        r"(?:현재|기말|잔여)\s*미상환\s*(?:사채\s*)?"
        r"(?:잔액|액면\s*금액|총액|액면\s*총액)\s*[:：\s]*([\d,]+)\s*원?"
    ),
    re.compile(
        r"미상환\s*사채\s*(?:액면\s*총액|총액|잔액)\s*[:：\s]*([\d,]+)\s*원?"
    ),
]
EVENT_PATTERNS = [
    (re.compile(r"전환청구권행사"), "conversion"),
    (re.compile(r"신주인수권행사"), "warrant_exercise"),
    (re.compile(r"교환청구권행사"), "exchange"),
    (re.compile(r"조기상환청구권행사|사채권\s*조기상환|사채\s*조기상환"), "put"),
    (re.compile(r"사채권\s*매도청구|매도청구권행사|콜옵션행사"), "call"),
    (re.compile(r"만기상환|사채\s*만기"), "redemption"),
    (re.compile(r"^\[정정\].*전환사채|^\[기재정정\].*전환사채"), "correction"),
]

ENDPOINTS = {
    "CB": "https://opendart.fss.or.kr/api/cvbdIsDecsn.json",
    "BW": "https://opendart.fss.or.kr/api/bdwtIsDecsn.json",
    "EB": "https://opendart.fss.or.kr/api/exbdIsDecsn.json",
}

WORKERS = 8
TIMEOUT = 15
RETRY_COUNT = 2

# Field aliases — DART's response key naming varies across the three
# endpoints (CB / BW / EB) and across legacy filings. We try each alias
# in order and take the first non-empty value.
#
# Source: OpenDART API guide (cvbdIsDecsn / bdwtIsDecsn / exbdIsDecsn).
# Earlier mapping had typos (bd_int_ex vs the real bd_intr_ex) and was
# missing BW/EB-specific price/ratio/exercise-period keys, which caused
# all date / rate fields to come back null in production.
FIELDS = {
    "round": ["bd_tm", "bd_knd", "bd_kind", "kind", "tm"],
    "face_amount": ["bd_fta", "bd_isu_amt", "isu_amt", "bdfta"],
    "currency": ["fdrm_crncy", "ovis_fdrm_crncy", "crncy"],
    # 납입일 (CB/BW/EB 공통).
    "issue_date": ["pymd", "pay_dt", "pymd_dt", "bd_isu_dt", "isu_dt"],
    # 만기일 — DART 실제 응답 키는 bd_mtd. 과거 자체 추측이었던
    # mtrt_dt / bd_mtrt_dt / mty_dt는 실데이터에서 한 번도 안 채워졌음.
    "maturity": ["bd_mtd", "mtrt_dt", "bd_mtrt_dt", "mty_dt"],
    # 표면이자율 / 만기이자율 — 실제 키는 bd_intr_ex / bd_intr_sf.
    "coupon_rate": ["bd_intr_ex", "bd_int_ex", "sl_intr_rt", "cpn_rt"],
    "ytm_rate": ["bd_intr_sf", "bd_int_sf", "mtrt_intr_rt", "ytm_rt"],
    # 가격 / 비율: CB는 cv_prc / cv_rt, BW는 wt_prc / wt_rt (신주인수권
    # 행사가액·비율), EB는 ex_prc / ex_rt (교환가액·비율).
    "conversion_price": ["cv_prc", "wt_prc", "ex_prc", "cnv_prc", "exrc_prc"],
    "conversion_ratio": ["cv_rt", "wt_rt", "ex_rt", "cnv_rt", "exrc_rt"],
    # 청구·행사기간: CB cv_rqsr_*, BW wt_exrc_pd_*, EB ex_rqsr_*.
    "convert_start": [
        "cv_rqsr_bgd", "wt_exrc_pd_bgn", "ex_rqsr_bgd",
        "cnv_pd_bgn", "exrc_pd_bgn",
    ],
    "convert_end": [
        "cv_rqsr_edd", "wt_exrc_pd_edd", "ex_rqsr_edd",
        "cnv_pd_end", "exrc_pd_end",
    ],
    # 사채매수청구권(풋) 행사가능 기간 — CB/BW/EB 공통.
    "put_start": ["pthbd_rcsr_bgd", "pt_pd_bgn", "ery_rdmpt_pd_bgn"],
    "put_end": ["pthbd_rcsr_edd", "pt_pd_end", "ery_rdmpt_pd_end"],
    # 매도청구권(콜) 행사가능 기간.
    "call_start": ["clbd_rcsr_bgd", "cl_pd_bgn"],
    "call_end": ["clbd_rcsr_edd", "cl_pd_end"],
}


def make_session():
    s = requests.Session()
    s.headers.update({"User-Agent": "my-dashboard/1.0 (mezzanine cron)"})
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
    # DART returns a JSON error body (HTTP 200) instead of a zip when the
    # API key is invalid, expired, or the daily quota is exhausted. Detect
    # that case explicitly so the failure surfaces an actionable message
    # instead of an opaque zipfile.BadZipFile traceback.
    if not r.content.startswith(b"PK"):
        try:
            body = r.json()
            raise RuntimeError(
                f"corpCode.xml not a zip — DART responded with "
                f"status={body.get('status')!r} message={body.get('message')!r}"
            )
        except ValueError:
            raise RuntimeError(
                f"corpCode.xml not a zip — first 200 bytes: "
                f"{r.content[:200]!r}"
            )
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


def _pick(row, names):
    for n in names:
        v = row.get(n)
        if v is None:
            continue
        s = str(v).strip()
        if s and s != "-":
            return s
    return None


def _to_int_amount(s):
    if not s:
        return None
    s = s.replace(",", "").replace(" ", "")
    # Sometimes amounts come with currency suffix like "5,000,000,000원"
    s = re.sub(r"[^\d.\-]", "", s)
    if not s:
        return None
    try:
        return int(float(s))
    except ValueError:
        return None


def _to_float_pct(s):
    if not s:
        return None
    s = s.replace(",", "").replace("%", "").strip()
    try:
        return float(s)
    except ValueError:
        return None


def _to_iso_date(s):
    if not s:
        return None
    s = str(s).strip()
    # "2024년 12월 31일" → "2024-12-31--" → strip; also handle stray spaces.
    s = re.sub(r"[년월]", "-", s)
    s = s.replace("일", "")
    s = re.sub(r"\s+", "", s)
    # Accept YYYY-MM-DD, YYYY.MM.DD, YYYY/MM/DD, YYYYMMDD.
    m = re.match(r"^(\d{4})[\-./]?(\d{1,2})[\-./]?(\d{1,2})", s)
    if not m:
        return None
    y, mo, d = m.group(1), m.group(2).zfill(2), m.group(3).zfill(2)
    return f"{y}-{mo}-{d}"


def normalize_issuance(row, kind):
    n = {
        "type": kind,
        "rceptNo": (row.get("rcept_no") or "").strip() or None,
        "rceptDt": _to_iso_date(row.get("rcept_dt") or ""),
        "round": _pick(row, FIELDS["round"]),
        "issueDate": _to_iso_date(_pick(row, FIELDS["issue_date"]) or ""),
        "maturityDate": _to_iso_date(_pick(row, FIELDS["maturity"]) or ""),
        "faceAmount": _to_int_amount(_pick(row, FIELDS["face_amount"])),
        "currency": _pick(row, FIELDS["currency"]) or "KRW",
        "couponRate": _to_float_pct(_pick(row, FIELDS["coupon_rate"])),
        "ytmRate": _to_float_pct(_pick(row, FIELDS["ytm_rate"])),
        "conversionPrice": _to_int_amount(
            _pick(row, FIELDS["conversion_price"])
        ),
        "conversionRatio": _to_float_pct(
            _pick(row, FIELDS["conversion_ratio"])
        ),
        "convertStart": _to_iso_date(_pick(row, FIELDS["convert_start"]) or ""),
        "convertEnd": _to_iso_date(_pick(row, FIELDS["convert_end"]) or ""),
        "putStart": _to_iso_date(_pick(row, FIELDS["put_start"]) or ""),
        "putEnd": _to_iso_date(_pick(row, FIELDS["put_end"]) or ""),
        "callStart": _to_iso_date(_pick(row, FIELDS["call_start"]) or ""),
        "callEnd": _to_iso_date(_pick(row, FIELDS["call_end"]) or ""),
    }
    return n


def categorize_event(title):
    for pat, kind in EVENT_PATTERNS:
        if pat.search(title):
            return kind
    return None


def extract_round(text):
    if not text:
        return None
    m = ROUND_RE.search(text)
    return m.group(1) if m else None


def scan_events(sess, corp_code, bgn_de, end_de):
    """List.json scan for mezzanine action events. Title patterns only —
    body parsing for amounts is deferred to v3. Returns list of dicts."""
    events = []
    for page_no in range(1, LIST_MAX_PAGES + 1):
        params = {
            "crtfc_key": DART_KEY,
            "corp_code": corp_code,
            "bgn_de": bgn_de,
            "end_de": end_de,
            "page_count": LIST_PAGE_SIZE,
            "page_no": page_no,
        }
        try:
            r = sess.get(LIST_URL, params=params, timeout=TIMEOUT)
            if r.status_code != 200:
                break
            body = r.json()
            status = body.get("status")
            if status == "013":
                break
            if status != "000":
                break
            for row in (body.get("list") or []):
                title = (row.get("report_nm") or "").strip()
                kind = categorize_event(title)
                if not kind:
                    continue
                events.append({
                    "date": _to_iso_date(row.get("rcept_dt") or ""),
                    "rceptNo": (row.get("rcept_no") or "").strip() or None,
                    "title": title,
                    "type": kind,
                    "round": extract_round(title),
                })
            total_page = int(body.get("total_page") or 1)
            if page_no >= total_page:
                break
        except (requests.RequestException, ValueError):
            break
    # Newest first.
    events.sort(key=lambda e: e["date"] or "0000-00-00", reverse=True)
    return events


def fetch_disclosure_body(sess, rcept_no):
    """OpenDART document.xml returns a ZIP with one or more XML files.
    Strip tags and concatenate the text so regex can run over a single
    blob. Returns None on any failure — caller falls back to face amount."""
    if not rcept_no:
        return None
    for attempt in range(RETRY_COUNT + 1):
        try:
            r = sess.get(
                DOCUMENT_URL,
                params={"crtfc_key": DART_KEY, "rcept_no": rcept_no},
                timeout=TIMEOUT,
            )
            if r.status_code != 200:
                return None
            try:
                zf = zipfile.ZipFile(io.BytesIO(r.content))
            except zipfile.BadZipFile:
                return None
            chunks = []
            for name in zf.namelist():
                try:
                    raw = zf.read(name)
                except KeyError:
                    continue
                try:
                    text = raw.decode("utf-8")
                except UnicodeDecodeError:
                    text = raw.decode("cp949", errors="replace")
                # Collapse XML tags into spaces so adjacent header/value
                # cells stay separated for regex.
                text = re.sub(r"<[^>]+>", " ", text)
                # Decode common XML entities the lazy way.
                text = (text
                        .replace("&nbsp;", " ")
                        .replace("&#160;", " ")
                        .replace("&amp;", "&"))
                chunks.append(text)
            return "\n".join(chunks)
        except requests.RequestException:
            time.sleep(0.5 * (attempt + 1))
            continue
    return None


def parse_outstanding_from_body(text):
    if not text:
        return None
    for pat in OUTSTANDING_PATTERNS:
        for m in pat.finditer(text):
            raw = m.group(1).replace(",", "").strip()
            if not raw:
                continue
            try:
                amount = int(raw)
            except ValueError:
                continue
            # 0 is legitimate (fully converted/redeemed). Otherwise CB
            # face amounts are at least 1억 — anything smaller is noise
            # the regex picked up from a column index or a fee figure.
            if amount == 0 or amount >= 100_000_000:
                return amount
    return None


def enrich_with_outstanding(sess, issuances):
    """For each issuance with action events, parse the latest non-correction
    event's disclosure body to recover the post-exercise outstanding face
    amount. Sets x['currentOutstanding'] when successful, plus a small
    provenance dict so the UI can show source."""
    for x in issuances:
        events = x.get("events") or []
        action = next(
            (e for e in events if e["type"] != "correction" and e.get("rceptNo")),
            None,
        )
        if not action:
            continue
        # Maturity / call by maturity → bond is fully redeemed. Skip the
        # body fetch; outstanding is 0.
        if action["type"] == "redemption":
            x["currentOutstanding"] = 0
            x["outstandingSource"] = {
                "rceptNo": action["rceptNo"],
                "date": action["date"],
                "method": "matured",
            }
            continue
        body = fetch_disclosure_body(sess, action["rceptNo"])
        amount = parse_outstanding_from_body(body)
        if amount is None:
            continue
        # Sanity: outstanding cannot exceed face amount. If it does,
        # the regex caught the wrong number — discard.
        if x.get("faceAmount") and amount > x["faceAmount"]:
            continue
        x["currentOutstanding"] = amount
        x["outstandingSource"] = {
            "rceptNo": action["rceptNo"],
            "date": action["date"],
            "method": "parsed",
        }


def attach_events(issuances, events):
    """Match each event to an issuance by 회차 number when possible.
    Unmatched events stay at ticker level so the UI can still show them."""
    by_round = {}
    for x in issuances:
        rnd = extract_round(x.get("round") or "")
        x["_round"] = rnd
        x["events"] = []
        if rnd:
            by_round.setdefault(rnd, []).append(x)
    unmatched = []
    for ev in events:
        r = ev["round"]
        targets = by_round.get(r) if r else None
        if targets:
            # If multiple issuances share the same round (shouldn't happen
            # but does when 회차 numbering resets across CB types), attach
            # to the one whose type matches the event most plausibly.
            best = targets[0]
            if len(targets) > 1:
                if ev["type"] in ("conversion",):
                    best = next((t for t in targets if t["type"] == "CB"), best)
                elif ev["type"] in ("warrant_exercise",):
                    best = next((t for t in targets if t["type"] == "BW"), best)
                elif ev["type"] in ("exchange",):
                    best = next((t for t in targets if t["type"] == "EB"), best)
            best["events"].append(ev)
        else:
            unmatched.append(ev)
    # Drop scratch field.
    for x in issuances:
        x.pop("_round", None)
    return unmatched


def summarize(issuances, unmatched_events):
    today = date.today().isoformat()
    total = 0
    outstanding = 0
    next_put = None
    total_events = sum(len(x.get("events") or []) for x in issuances)
    total_events += len(unmatched_events or [])
    parsed_count = 0
    for x in issuances:
        face = x.get("faceAmount") or 0
        total += face
        # Prefer the parsed post-event outstanding when available.
        # Otherwise fall back to face amount when maturity is still in
        # the future. Matured issuances with no parsed outstanding are
        # treated as 0 (we know they redeemed at maturity).
        if "currentOutstanding" in x:
            outstanding += x["currentOutstanding"]
            parsed_count += 1
        elif x.get("maturityDate") and x["maturityDate"] > today:
            outstanding += face
        ps = x.get("putStart")
        if ps and ps > today:
            if next_put is None or ps < next_put["date"]:
                next_put = {"date": ps, "amount": face}
    return {
        "totalIssued": total,
        "outstanding": outstanding,
        "outstandingParsedCount": parsed_count,
        "issuanceCount": len(issuances),
        "totalEvents": total_events,
        "nextPut": next_put,
    }


def fetch_kind(sess, corp_code, kind, bgn_de, end_de):
    url = ENDPOINTS[kind]
    params = {
        "crtfc_key": DART_KEY,
        "corp_code": corp_code,
        "bgn_de": bgn_de,
        "end_de": end_de,
    }
    for attempt in range(RETRY_COUNT + 1):
        try:
            r = sess.get(url, params=params, timeout=TIMEOUT)
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
            if rows:
                _record_sample(kind, rows[0])
            return [normalize_issuance(row, kind) for row in rows]
        except requests.RequestException:
            time.sleep(0.5 * (attempt + 1))
            continue
    return []


def fetch_one(sess, ticker, name, corp_code, bgn_de, end_de):
    issuances = []
    for kind in ENDPOINTS:
        issuances.extend(fetch_kind(sess, corp_code, kind, bgn_de, end_de))
    if not issuances:
        return None
    # Sort newest first (by 공시일).
    issuances.sort(
        key=lambda x: x["rceptDt"] or "0000-00-00",
        reverse=True,
    )
    # Only scan list.json for tickers that actually issued mezzanine —
    # avoids 2K+ wasted calls per run.
    events = scan_events(sess, corp_code, bgn_de, end_de)
    unmatched = attach_events(issuances, events)
    # For issuances with action events, fetch + parse the latest event's
    # body to recover the actual post-exercise outstanding face amount.
    enrich_with_outstanding(sess, issuances)
    return {
        "name": name,
        "corpCode": corp_code,
        "issuances": issuances,
        "unmatchedEvents": unmatched,
        "summary": summarize(issuances, unmatched),
    }


def main():
    if not DART_KEY:
        print("ERROR: OPENDART_KEY env var not set", file=sys.stderr)
        sys.exit(1)

    sess = make_session()
    print("Fetching DART corp_code master…", flush=True)
    try:
        code_map = download_corp_code_map(sess)
    except (RuntimeError, requests.RequestException, zipfile.BadZipFile) as e:
        print(f"ERROR: corp_code download failed — {e}", file=sys.stderr)
        sys.exit(1)
    print(f"  loaded {len(code_map):,} listed corp_code entries", flush=True)

    tickers = load_tickers()
    print(f"Loaded {len(tickers):,} tickers", flush=True)

    today = datetime.now(KST).date()
    bgn = today.replace(year=today.year - LOOKBACK_YEARS).strftime("%Y%m%d")
    end = today.strftime("%Y%m%d")
    print(f"Range {bgn} → {end} (CB/BW/EB)…", flush=True)

    results = {}
    skipped_no_corp = 0
    total_issuances = 0
    total_events = 0
    total_parsed = 0

    def task(ticker, name):
        cc = code_map.get(ticker)
        if not cc:
            return ticker, None, "no_corp"
        entry = fetch_one(sess, ticker, name, cc, bgn, end)
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
                total_issuances += entry["summary"]["issuanceCount"]
                total_events += entry["summary"]["totalEvents"]
                total_parsed += entry["summary"]["outstandingParsedCount"]
            elif reason == "no_corp":
                skipped_no_corp += 1
            if done % 200 == 0:
                print(
                    f"  {done}/{total} · with_mezz={len(results)} "
                    f"issuances={total_issuances} events={total_events} "
                    f"parsed={total_parsed} no_corp={skipped_no_corp}",
                    flush=True,
                )

    payload = {
        "asOf": datetime.now(KST).isoformat(timespec="seconds"),
        "rangeStart": bgn,
        "rangeEnd": end,
        "count": len(tickers),
        "issuersWithMezzanine": len(results),
        "totalIssuances": total_issuances,
        "totalEvents": total_events,
        "totalOutstandingParsed": total_parsed,
        "mezzanine": results,
    }
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, separators=(",", ":"))

    print(
        f"Wrote {OUTPUT_FILE} · issuers={len(results)} "
        f"issuances={total_issuances} events={total_events} "
        f"parsed_outstanding={total_parsed}",
        flush=True,
    )

    # Per-field fill rate. A field that suddenly drops to 0% almost always
    # means DART renamed a response key — easier to catch in CI than to
    # discover after the page has been showing dashes for weeks.
    counts = {}
    sample_total = 0
    for entry in results.values():
        for iss in entry.get("issuances", []):
            sample_total += 1
            for k, v in iss.items():
                if k in ("events", "outstandingSource", "currentOutstanding"):
                    continue
                counts.setdefault(k, 0)
                if v not in (None, "", []):
                    counts[k] += 1
    if sample_total:
        print("Field fill rates:", flush=True)
        suspect = []
        for k in sorted(counts):
            pct = 100.0 * counts[k] / sample_total
            warn = "  <-- SUSPECT" if pct < 5 else ""
            if pct < 5:
                suspect.append(k)
            print(
                f"  {k:<22} {counts[k]:>6}/{sample_total:<6} ({pct:5.1f}%){warn}",
                flush=True,
            )
        # Auto-dump raw response samples when something looks broken so the
        # next debugging round can update FIELDS without another probe run.
        if suspect and _SAMPLE_ROWS:
            print(
                f"\nSUSPECT fields ({len(suspect)}): {', '.join(suspect)}",
                flush=True,
            )
            print("Raw response samples (first row per endpoint):", flush=True)
            for kind in sorted(_SAMPLE_ROWS):
                row = _SAMPLE_ROWS[kind]
                print(f"  --- {kind} (keys={len(row)}) ---", flush=True)
                for k in sorted(row):
                    v = str(row[k]).replace("\n", " ")[:80]
                    print(f"    {k}: {v}", flush=True)


if __name__ == "__main__":
    main()
