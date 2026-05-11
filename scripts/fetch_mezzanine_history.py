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

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TICKERS_FILE = os.path.join(ROOT, "tickers-full.js")
OUTPUT_FILE = os.path.join(ROOT, "data", "mezzanine.json")
# Per-rceptNo cache of disclosure-body parse results. Disclosure content
# is immutable once filed, so once parsed we never need to fetch the
# body again. First run is slow (~20 min for ~1500 fetches), subsequent
# runs only fetch new disclosures (~1-2 min).
BODY_CACHE_FILE = os.path.join(ROOT, "data", "mezzanine-body-cache.json")

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

# Put-option exercise schedule isn't returned in the structured DART
# response — it lives in the disclosure body. Filers' phrasing varies
# wildly so we can't rely on a single labeled pattern. Strategy:
#
#   1. Find the put-keyword position in the body.
#   2. Open a ±-window around it (filings often state the dates before
#      the keyword: "...2025년 1월 1일부터 ... 사채매수청구권을 ...").
#   3. Try labeled extraction first (table-style "행사가능기간 시작일 : …").
#   4. Fall back to a heuristic: gather every date in the window, drop
#      ones close to the known issueDate / maturityDate (those are noise:
#      "발행일(2024-01-01)…만기일(2027-01-01)…"), and the earliest /
#      latest survivors are the put start / end.
#
# This gets ~10x the fill rate of a pure labeled-pattern approach on
# real DART filings (most CBs don't use the "행사가능기간 시작일" label).
PUT_KEYWORD_RE = re.compile(r"사채매수청구권|조기상환청구권|풋옵션")
CALL_KEYWORD_RE = re.compile(r"사채매도청구권|매도청구권|콜옵션")

# Common section breaks shared between put and call parsers. The
# per-keyword stop_re also includes the OTHER option's keyword so
# parsing one section doesn't leak into the other (real DART filings
# put 조기상환청구권 immediately followed by 매수인의 매도청구권).
_COMMON_BREAKS = (
    r"합병에?\s*관한\s*사항|"
    r"청약일|납입일|납입방법|보증기관|담보제공|대표주관회사|"
    r"이사회결의일|사외이사\s*참석|감사위원\s*참석"
)
PUT_PARSE_STOP_RE = re.compile(
    _COMMON_BREAKS + r"|매수인의?\s*매도청구권|콜옵션|매도청구권"
)
CALL_PARSE_STOP_RE = re.compile(
    _COMMON_BREAKS + r"|조기상환청구권|사채매수청구권|풋옵션"
)

# % of 전자등록금액 (face amount) that the holder / issuer can demand
# in a single exercise. Most CB bodies write this as "전부 또는 일부"
# (qualitative) but some specify a numeric cap, hence both shapes.
PCT_PATTERN = re.compile(
    r"(?:전자등록금액|사채\s*권면\s*금액|권면\s*총액)\s*의?\s*"
    r"(전부\s*(?:또는\s*일부)?|일부|\d{1,3}\s*(?:\.\d+)?\s*%)"
)

_DATE_PAT = (
    r"(\d{4})\s*[년\-./]\s*(\d{1,2})\s*[월\-./]?\s*(\d{1,2})\s*일?"
)
_DATE_RE = re.compile(_DATE_PAT)

# Labeled-extraction patterns (Strategy 3 above). Tried first because
# they're high-confidence when present.
PUT_START_LABEL_PATTERNS = [
    re.compile(
        r"(?:행사\s*가능\s*기간\s*시작일?|행사\s*시작일?|개시일?|"
        r"최초\s*행사일?|첫\s*행사일?)\s*[:：]?\s*" + _DATE_PAT
    ),
    re.compile(
        r"발행일.{0,30}경과한?\s*날\s*[\(（]\s*" + _DATE_PAT + r"\s*[\)）]"
    ),
]
PUT_END_LABEL_PATTERNS = [
    re.compile(
        r"(?:행사\s*가능\s*기간\s*종료일?|행사\s*종료일?|만료일?|"
        r"마지막\s*행사일?)\s*[:：]?\s*" + _DATE_PAT
    ),
    re.compile(
        r"만기일?\s*\d+\s*개월?\s*전\s*[\(（]\s*" + _DATE_PAT + r"\s*[\)）]"
    ),
]

# Parse outcome diagnostics. A non-trivial chunk of issuances inevitably
# stay un-parsed (filers using unusual phrasing); the counters + sample
# chunks make it cheap to iterate on patterns when fill rate is too low.
_PARSE_DIAGS = {
    "ok": 0,
    "no_body": 0,
    "no_keyword": 0,
    "no_dates": 0,
    "filtered_out": 0,
    "cache_hit": 0,
}
_DIAGS_LOCK = threading.Lock()
_NEAR_MISS_SAMPLES = []  # up to 5 short excerpts where keyword found but no put dates

# Body parse cache: rcept_no -> {putStart, putEnd, putPct, callStart,
# callEnd, callPct, outcome}. Keys with None values are stripped on
# write to keep the file compact. "no_body" outcomes are NOT cached —
# those can be transient (rate limit / network) and may succeed on a
# later run.
_BODY_CACHE = {}
_BODY_CACHE_DIRTY = False
_BODY_CACHE_LOCK = threading.Lock()


def load_body_cache():
    global _BODY_CACHE
    try:
        with open(BODY_CACHE_FILE, encoding="utf-8") as f:
            _BODY_CACHE = json.load(f)
    except (FileNotFoundError, ValueError):
        _BODY_CACHE = {}
    return len(_BODY_CACHE)


def save_body_cache():
    if not _BODY_CACHE_DIRTY:
        return False
    os.makedirs(os.path.dirname(BODY_CACHE_FILE), exist_ok=True)
    with open(BODY_CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(
            _BODY_CACHE, f, ensure_ascii=False,
            separators=(",", ":"), sort_keys=True,
        )
    return True


def _cache_get(rcept_no):
    with _BODY_CACHE_LOCK:
        return _BODY_CACHE.get(rcept_no)


def _cache_put(rcept_no, entry):
    global _BODY_CACHE_DIRTY
    compact = {k: v for k, v in entry.items() if v is not None}
    with _BODY_CACHE_LOCK:
        _BODY_CACHE[rcept_no] = compact
        _BODY_CACHE_DIRTY = True


def _record_parse(outcome):
    """outcome is either a bare string ('ok' / 'no_body' / 'no_keyword')
    or a (string, chunk) tuple for near-miss cases where we want to keep
    a chunk excerpt for later pattern refinement."""
    if isinstance(outcome, tuple):
        key, chunk = outcome
    else:
        key, chunk = outcome, None
    with _DIAGS_LOCK:
        _PARSE_DIAGS[key] = _PARSE_DIAGS.get(key, 0) + 1
        if (key in ("no_dates", "filtered_out")
                and chunk
                and len(_NEAR_MISS_SAMPLES) < 5):
            _NEAR_MISS_SAMPLES.append(chunk[:500])

ENDPOINTS = {
    "CB": "https://opendart.fss.or.kr/api/cvbdIsDecsn.json",
    "BW": "https://opendart.fss.or.kr/api/bdwtIsDecsn.json",
    "EB": "https://opendart.fss.or.kr/api/exbdIsDecsn.json",
}

WORKERS = 8
TIMEOUT = 15
RETRY_COUNT = 3

# DART status codes returned in the JSON body. Treating them three ways:
#   - hard-fail (don't retry, return empty for this call)
#   - retryable transient (light backoff)
#   - retryable rate-limited (longer exponential backoff)
# See https://opendart.fss.or.kr/guide/main.do.
HARD_FAIL_STATUSES = frozenset({"010", "011", "012", "013", "014"})
TRANSIENT_STATUSES = frozenset({"100", "101", "800", "900"})
RATELIMIT_STATUSES = frozenset({"020"})

# Counter of API calls that returned empty after exhausting retries.
# Surfaced at the end of main() so silent data loss is visible.
_API_FAILURES = {"ratelimit": 0, "transient": 0, "http_error": 0, "network": 0}
_FAILURES_LOCK = threading.Lock()


def _record_failure(kind):
    with _FAILURES_LOCK:
        _API_FAILURES[kind] = _API_FAILURES.get(kind, 0) + 1

# Field aliases — DART's response key naming varies across the three
# endpoints (CB / BW / EB). Verified against actual response samples
# captured by the in-script probe (commit 6c72131).
#
# What's confirmed in the structured response:
#   - 발행조건 (face, coupon, ytm, maturity, issue date, conversion price/ratio)
#   - 전환청구·행사기간 (cvrqpd_* for CB, expd_* for BW, exrqpd_* for EB)
# What's NOT in the structured response (would need disclosure body parsing):
#   - 사채매수청구권(풋) 행사 기간
#   - 매도청구권(콜) 행사 기간
#   - 공시 접수일자 (rcept_dt) — derive from rcept_no's YYYYMMDD prefix instead
FIELDS = {
    "round": ["bd_tm", "bd_knd", "bd_kind", "kind", "tm"],
    "face_amount": ["bd_fta", "bd_isu_amt", "isu_amt", "bdfta"],
    "currency": ["fdrm_crncy", "ovis_fdrm_crncy", "crncy"],
    "issue_date": ["pymd", "pay_dt", "pymd_dt", "bd_isu_dt", "isu_dt"],
    "maturity": ["bd_mtd", "mtrt_dt", "bd_mtrt_dt", "mty_dt"],
    "coupon_rate": ["bd_intr_ex", "bd_int_ex", "sl_intr_rt", "cpn_rt"],
    "ytm_rate": ["bd_intr_sf", "bd_int_sf", "mtrt_intr_rt", "ytm_rt"],
    "conversion_price": ["cv_prc", "wt_prc", "ex_prc", "cnv_prc", "exrc_prc"],
    "conversion_ratio": ["cv_rt", "wt_rt", "ex_rt", "cnv_rt", "exrc_rt"],
    # 청구·행사기간:
    #   CB cvrqpd_*  (전환청구기간)
    #   BW expd_*    (신주인수권 행사기간)
    #   EB exrqpd_*  (교환청구기간 — symmetric guess; verify if 0% next run)
    "convert_start": [
        "cvrqpd_bgd", "expd_bgd", "exrqpd_bgd",
        # legacy / undocumented aliases kept as safety fallback:
        "cv_rqsr_bgd", "wt_exrc_pd_bgn", "ex_rqsr_bgd",
    ],
    "convert_end": [
        "cvrqpd_edd", "expd_edd", "exrqpd_edd",
        "cv_rqsr_edd", "wt_exrc_pd_edd", "ex_rqsr_edd",
    ],
    # NOTE: put_*/call_* aliases below cover the rare older filing formats
    # that did embed exercise periods in the structured response. Modern
    # CB/BW/EB filings keep these in narrative-only — body parsing TODO.
    "put_start": ["pthbd_rcsr_bgd", "pt_pd_bgn", "ery_rdmpt_pd_bgn"],
    "put_end": ["pthbd_rcsr_edd", "pt_pd_end", "ery_rdmpt_pd_end"],
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
    rcept_no = (row.get("rcept_no") or "").strip() or None
    rcept_dt = _to_iso_date(row.get("rcept_dt") or "")
    # Disclosure detail endpoints (cvbdIsDecsn etc.) don't return rcept_dt;
    # DART encodes the receipt date as the first 8 chars of rcept_no.
    if not rcept_dt and rcept_no and len(rcept_no) >= 8 and rcept_no[:8].isdigit():
        rcept_dt = _to_iso_date(rcept_no[:8])
    n = {
        "type": kind,
        "rceptNo": rcept_no,
        "rceptDt": rcept_dt,
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
        body = None
        for attempt in range(RETRY_COUNT + 1):
            try:
                r = sess.get(LIST_URL, params=params, timeout=TIMEOUT)
                if r.status_code == 429 or 500 <= r.status_code < 600:
                    time.sleep(2 * (attempt + 1))
                    continue
                if r.status_code != 200:
                    break
                body = r.json()
                status = body.get("status")
                if status in RATELIMIT_STATUSES:
                    time.sleep(2 ** (attempt + 1))
                    continue
                if status in TRANSIENT_STATUSES:
                    time.sleep(0.5 * (attempt + 1))
                    continue
                break
            except (requests.RequestException, ValueError):
                time.sleep(0.5 * (attempt + 1))
                continue
        if not body:
            break
        status = body.get("status")
        if status == "013":
            break
        if status != "000":
            _record_failure("ratelimit" if status in RATELIMIT_STATUSES
                            else "http_error")
            break
        try:
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
            if r.status_code == 429 or 500 <= r.status_code < 600:
                time.sleep(2 * (attempt + 1))
                continue
            if r.status_code != 200:
                return None
            try:
                zf = zipfile.ZipFile(io.BytesIO(r.content))
            except zipfile.BadZipFile:
                # Often a JSON error body (rate limit) instead of a ZIP.
                # Check and back off if so.
                try:
                    body = r.json()
                    if body.get("status") in RATELIMIT_STATUSES:
                        time.sleep(2 ** (attempt + 1))
                        continue
                except ValueError:
                    pass
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


def _abs_day_gap(iso_a, iso_b):
    """Days between two YYYY-MM-DD strings (None-safe). Used to detect
    'close enough' matches to known issueDate / maturityDate so heuristic
    date extraction doesn't pick them up as put dates."""
    if not iso_a or not iso_b:
        return 10_000
    try:
        a = date.fromisoformat(iso_a)
        b = date.fromisoformat(iso_b)
        return abs((a - b).days)
    except ValueError:
        return 10_000


def _all_dates_in(chunk):
    out = []
    for m in _DATE_RE.finditer(chunk):
        y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if not (1990 <= y <= 2099 and 1 <= mo <= 12 and 1 <= d <= 31):
            continue
        out.append(f"{y:04d}-{mo:02d}-{d:02d}")
    return out


def _try_labeled(chunk, patterns):
    for pat in patterns:
        m = pat.search(chunk)
        if not m:
            continue
        # Patterns use three capturing groups (Y/M/D).
        y, mo, d = m.group(1), m.group(2).zfill(2), m.group(3).zfill(2)
        try:
            iso = f"{int(y):04d}-{int(mo):02d}-{int(d):02d}"
            if 1990 <= int(y) <= 2099:
                return iso
        except ValueError:
            continue
    return None


def parse_put_schedule(text, issue_date_iso, maturity_date_iso, keyword_re,
                       start_label_pats, end_label_pats,
                       stop_re=None):
    """Extract (start, end, pct) put-style exercise schedule from a
    disclosure body. Returns ((start_iso, end_iso, pct), outcome).
    The pct string is the human phrase ("전부 또는 일부", "70%", etc.) —
    callers normalize for display.

    See module-level comment for the strategy.
    """
    if not text:
        return None, None, None, "no_body"
    m = keyword_re.search(text)
    if not m:
        return None, None, None, "no_keyword"
    # Real DART filings always put the section heading FIRST then the
    # narrative — forward-only window is correct. 100-char backward
    # buffer covers tabular cases where a cell label (e.g.,
    # "행사가능기간") sits to the left of the dates after XML strip.
    # Truncate forward at the next section heading so put doesn't leak
    # into call and vice versa (back-to-back in real filings).
    lo = max(0, m.start() - 100)
    hi = min(len(text), m.start() + 1500)
    if stop_re:
        bm_fwd = stop_re.search(text, m.end())
        if bm_fwd and bm_fwd.start() < hi:
            hi = bm_fwd.start()
    chunk = text[lo:hi]
    pct_match = PCT_PATTERN.search(chunk)
    pct = pct_match.group(1).strip() if pct_match else None
    if pct:
        # Normalize: collapse internal whitespace.
        pct = re.sub(r"\s+", "", pct)

    # Strategy A: high-confidence labeled patterns.
    start_iso = _try_labeled(chunk, start_label_pats)
    end_iso = _try_labeled(chunk, end_label_pats)
    if start_iso and end_iso and end_iso < start_iso:
        end_iso = None

    # Strategy B: heuristic when labels missed.
    near_miss = None
    if not (start_iso and end_iso):
        all_dates = _all_dates_in(chunk)
        if not all_dates:
            if not start_iso:
                return None, None, pct, ("no_dates", chunk)
        else:
            # Drop dates close to known issue / maturity (those are echo
            # references like "발행일(2024-01-01)").
            # Filter out exact echoes of issue / maturity dates. DART
            # bodies use the exact ISO date when referencing the issue
            # or maturity (no 영업일 shifts), so an exact-match filter
            # is enough — wider buffers would falsely reject legitimate
            # puts that sit just before maturity.
            candidates = sorted(set(
                d for d in all_dates
                if d != issue_date_iso and d != maturity_date_iso
            ))
            if not candidates and not start_iso:
                return None, None, pct, ("filtered_out", chunk)
            if not start_iso and candidates:
                if issue_date_iso:
                    future = [d for d in candidates if d > issue_date_iso]
                    if future:
                        start_iso = future[0]
                    else:
                        start_iso = candidates[0]
                else:
                    start_iso = candidates[0]
                near_miss = chunk
            if not end_iso and candidates:
                later = [d for d in candidates if d != start_iso]
                if later:
                    end_iso = later[-1]
                if end_iso == start_iso:
                    end_iso = None

    # Sanity gates: put_start must be strictly after issue and before
    # maturity; put_end can't exceed maturity.
    if issue_date_iso and start_iso and start_iso <= issue_date_iso:
        return None, None, pct, ("filtered_out", chunk)
    if maturity_date_iso and start_iso and start_iso > maturity_date_iso:
        return None, None, pct, ("filtered_out", chunk)
    if maturity_date_iso and end_iso and end_iso > maturity_date_iso:
        end_iso = None

    if start_iso:
        return start_iso, end_iso, pct, "ok"
    return None, None, pct, ("no_dates", near_miss or chunk)


def enrich_with_put_schedule(sess, issuances):
    """For each not-yet-redeemed issuance, fetch its own disclosure body
    and extract put (and best-effort call) exercise schedule. Most CBs
    keep these as narrative or labeled tables in the body — they're
    absent from DART's structured JSON response.

    Skips issuances that are already past maturity or that
    enrich_with_outstanding marked as fully redeemed: nothing to put.
    """
    today = date.today().isoformat()
    for x in issuances:
        if x.get("putStart"):
            continue  # already populated via legacy structured key
        if x.get("currentOutstanding") == 0:
            continue
        if x.get("maturityDate") and x["maturityDate"] < today:
            continue
        rcept_no = x.get("rceptNo")
        if not rcept_no:
            continue

        # Cache hit: skip the body fetch entirely. Disclosure bodies
        # are immutable so cached results stay valid forever.
        cached = _cache_get(rcept_no)
        if cached is not None:
            _record_parse("cache_hit")
            if cached.get("putStart"):
                x["putStart"] = cached["putStart"]
                x["putSource"] = "body"
            for k in ("putEnd", "putPct", "callStart", "callEnd", "callPct"):
                if cached.get(k):
                    x[k] = cached[k]
            continue

        body = fetch_disclosure_body(sess, rcept_no)
        if not body:
            # Don't cache — body fetch failures may be transient.
            _record_parse("no_body")
            continue

        ps, pe, pct, outcome = parse_put_schedule(
            body, x.get("issueDate"), x.get("maturityDate"),
            PUT_KEYWORD_RE, PUT_START_LABEL_PATTERNS, PUT_END_LABEL_PATTERNS,
            stop_re=PUT_PARSE_STOP_RE,
        )
        _record_parse(outcome)
        # Same body usually mentions call schedule — reuse parser with
        # call stop_re. Diagnostics are put-centric so call outcomes
        # aren't separately counted.
        cs, ce, cpct, _ = parse_put_schedule(
            body, x.get("issueDate"), x.get("maturityDate"),
            CALL_KEYWORD_RE, PUT_START_LABEL_PATTERNS, PUT_END_LABEL_PATTERNS,
            stop_re=CALL_PARSE_STOP_RE,
        )
        outcome_key = outcome if isinstance(outcome, str) else outcome[0]
        _cache_put(rcept_no, {
            "putStart": ps, "putEnd": pe, "putPct": pct,
            "callStart": cs, "callEnd": ce, "callPct": cpct,
            "outcome": outcome_key,
        })
        if ps:
            x["putStart"] = ps
            x["putSource"] = "body"
        if pe:
            x["putEnd"] = pe
        if pct:
            x["putPct"] = pct
        if cs:
            x["callStart"] = cs
        if ce:
            x["callEnd"] = ce
        if cpct:
            x["callPct"] = cpct


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
    last_reason = "network"
    for attempt in range(RETRY_COUNT + 1):
        try:
            r = sess.get(url, params=params, timeout=TIMEOUT)
            if r.status_code == 429 or 500 <= r.status_code < 600:
                # HTTP-level rate limit / server error. Back off and retry.
                last_reason = "ratelimit" if r.status_code == 429 else "http_error"
                time.sleep(2 * (attempt + 1))
                continue
            if r.status_code != 200:
                _record_failure("http_error")
                return []
            body = r.json()
            status = body.get("status")
            if status == "000":
                rows = body.get("list") or []
                return [normalize_issuance(row, kind) for row in rows]
            if status in HARD_FAIL_STATUSES:
                # "013" = no data is legitimate. The others ("010" missing
                # key, "011" expired, "012" IP block, "014" no file) are
                # job-level fatal; surface in the silent-failure counter
                # so the run summary shows the cause.
                if status != "013":
                    _record_failure("http_error")
                return []
            if status in RATELIMIT_STATUSES:
                last_reason = "ratelimit"
                # 2s, 4s, 8s, 16s — DART rate limit window is ~1 minute.
                time.sleep(2 ** (attempt + 1))
                continue
            if status in TRANSIENT_STATUSES:
                last_reason = "transient"
                time.sleep(0.5 * (attempt + 1))
                continue
            # Unknown status — log as http_error so it doesn't silently
            # vanish, but don't retry.
            _record_failure("http_error")
            return []
        except requests.RequestException:
            last_reason = "network"
            time.sleep(0.5 * (attempt + 1))
            continue
    _record_failure(last_reason)
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
    # Put/call schedules aren't in the structured DART response — parse
    # them out of each live issuance's own disclosure body.
    enrich_with_put_schedule(sess, issuances)
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
    cache_loaded = load_body_cache()
    print(
        f"Loaded body-parse cache: {cache_loaded:,} entries",
        flush=True,
    )
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

    if save_body_cache():
        print(
            f"Wrote {BODY_CACHE_FILE} · {len(_BODY_CACHE):,} cached entries",
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
        # put/call schedules come from disclosure-body parsing (regex),
        # so partial fill is expected — only warn if essentially nothing
        # was extracted, which would mean the patterns broke entirely.
        body_parsed = {"putStart", "putEnd", "callStart", "callEnd"}
        print("Field fill rates:", flush=True)
        for k in sorted(counts):
            pct = 100.0 * counts[k] / sample_total
            if k in body_parsed:
                warn = "  <-- BODY PARSE MISSING" if pct < 1 else ""
            else:
                warn = "  <-- SUSPECT" if pct < 5 else ""
            print(
                f"  {k:<22} {counts[k]:>6}/{sample_total:<6} ({pct:5.1f}%){warn}",
                flush=True,
            )

    # Silent-failure summary. Non-zero ratelimit count means some
    # tickers' mezzanine quietly didn't make it into the data — re-run
    # the workflow to capture them, or reduce WORKERS if it persists.
    total_fail = sum(_API_FAILURES.values())
    if total_fail:
        print(
            f"\nAPI failures after retries: total={total_fail} "
            f"ratelimit={_API_FAILURES['ratelimit']} "
            f"http_error={_API_FAILURES['http_error']} "
            f"transient={_API_FAILURES['transient']} "
            f"network={_API_FAILURES['network']}",
            flush=True,
        )
        if _API_FAILURES["ratelimit"] > 100:
            print(
                "  WARN: high ratelimit count — DART throttled many calls. "
                "Re-trigger the workflow or lower WORKERS to recover the "
                "missing tickers.",
                flush=True,
            )

    # Put-schedule parse summary. Helps catch when filers change their
    # disclosure phrasing en masse (would show up as a spike in no_keyword
    # or no_dates). Near-miss samples are short body excerpts so the
    # next pattern iteration can see what real bodies look like without
    # another diagnostic round trip.
    parse_total = sum(_PARSE_DIAGS.values())
    if parse_total:
        print(
            f"\nPut schedule parse outcomes (per live issuance):",
            flush=True,
        )
        for k in ("ok", "cache_hit", "no_keyword", "no_dates",
                  "filtered_out", "no_body"):
            cnt = _PARSE_DIAGS.get(k, 0)
            pct = 100.0 * cnt / parse_total
            print(f"  {k:<14} {cnt:>5}/{parse_total:<5} ({pct:5.1f}%)",
                  flush=True)
        if _NEAR_MISS_SAMPLES:
            print(
                f"\nNear-miss body excerpts "
                f"({len(_NEAR_MISS_SAMPLES)}) — keyword found but no put "
                f"dates survived filtering:",
                flush=True,
            )
            for i, s in enumerate(_NEAR_MISS_SAMPLES, 1):
                # Squash whitespace for log readability.
                squashed = re.sub(r"\s+", " ", s)
                print(f"  --- sample {i} ---\n  {squashed}", flush=True)


if __name__ == "__main__":
    main()
