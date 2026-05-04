#!/usr/bin/env python3
"""Phase 2A — DART에서 IPO별 발행구조 데이터를 받아 ipo-history.json에 머지.

이번 PR 범위 (Phase 2A — 토대 작업):
- DART corpCode.xml 다운로드 → ticker(stock_code) ↔ corp_code 매핑 빌드
- 각 IPO에 대해 irdsSttus.json (증권발행실적보고서) 호출
- 받아온 구조화 필드를 ipo-history.json의 해당 listing 엔트리에 병합

이번 단계에선 DART가 실제 무엇을 돌려주는지 확인하는 게 목적. 받아본 후
추가 파싱이 필요하면 Phase 2B에서 증권신고서 본문(보호예수 일정) 파싱
까지 확장.

출력: data/ipo-history.json 의 각 listing에 'dart' 필드 추가 (없으면 생략).
"""

import io
import json
import os
import sys
import time
import zipfile
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

import requests

KST = timezone(timedelta(hours=9))

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INPUT_FILE = os.path.join(ROOT, "data", "ipo-history.json")
CORP_MAP_CACHE = os.path.join(ROOT, "data", "_dart_corp_map.json")

DART_KEY = os.environ.get("DART_API_KEY", "").strip()
DART_BASE = "https://opendart.fss.or.kr/api"

USER_AGENT = "Mozilla/5.0 my-dashboard-ipo-builder"

# 증권발행실적보고서 (정기보고서 reprt_code 매핑은 다름; irdsSttus는 단독 보고서)
IRDS_URL = f"{DART_BASE}/irdsSttus.json"

# corpCode endpoint — zip-of-xml
CORP_CODE_URL = f"{DART_BASE}/corpCode.xml"

# 동시성 — DART는 분당 1000 / 일당 20000 호출 한도 (개인 키)
WORKERS = 8


def require_key():
    if not DART_KEY:
        print("ERROR: DART_API_KEY env var is not set.", file=sys.stderr)
        print("       Set it in GitHub Secrets and reference via env in the workflow.", file=sys.stderr)
        sys.exit(1)


def make_session():
    s = requests.Session()
    s.headers.update({"User-Agent": USER_AGENT, "Accept": "application/json"})
    return s


def fetch_corp_code_mapping(session):
    """DART corpCode.xml (zip) 다운로드 → {stock_code: corp_code, ...} 매핑.
    하루 한 번이면 충분 — 캐시 있으면 24h 안엔 그대로 사용."""
    if os.path.exists(CORP_MAP_CACHE):
        mtime = os.path.getmtime(CORP_MAP_CACHE)
        if time.time() - mtime < 24 * 3600:
            with open(CORP_MAP_CACHE, "r", encoding="utf-8") as f:
                return json.load(f)
    print("DART corpCode.xml 다운로드 중...", flush=True)
    r = session.get(CORP_CODE_URL, params={"crtfc_key": DART_KEY}, timeout=60)
    r.raise_for_status()
    with zipfile.ZipFile(io.BytesIO(r.content)) as zf:
        with zf.open(zf.namelist()[0]) as f:
            tree = ET.parse(f)
    root = tree.getroot()
    mapping = {}
    for elem in root.iter("list"):
        stock_code = (elem.findtext("stock_code") or "").strip()
        corp_code = (elem.findtext("corp_code") or "").strip()
        if stock_code and len(stock_code) == 6 and corp_code:
            mapping[stock_code] = corp_code
    os.makedirs(os.path.dirname(CORP_MAP_CACHE), exist_ok=True)
    with open(CORP_MAP_CACHE, "w", encoding="utf-8") as f:
        json.dump(mapping, f)
    print(f"  매핑 {len(mapping)}개 저장", flush=True)
    return mapping


def fetch_irds_sttus(session, corp_code, bsns_year):
    """증권발행실적보고서 — 발행 종류 / 수량 / 의무보유 정보 일부 포함.
    reprt_code 11013 = 1분기, 11012 = 반기, 11014 = 3분기, 11011 = 사업보고서 (annual).
    annual을 우선 시도. annual이 없으면 None 반환."""
    params = {
        "crtfc_key": DART_KEY,
        "corp_code": corp_code,
        "bsns_year": str(bsns_year),
        "reprt_code": "11011",
    }
    try:
        r = session.get(IRDS_URL, params=params, timeout=20)
        if r.status_code != 200:
            return None
        j = r.json()
    except Exception:
        return None
    if j.get("status") != "000":
        return None  # 데이터 없음 (013) 또는 에러
    return j.get("list") or []


def summarize_irds(rows):
    """irdsSttus 응답에서 IPO에 의미있는 필드만 뽑아 요약."""
    if not rows:
        return None
    summary = {"events": []}
    for r in rows:
        # irdsSttus 필드 (전형):
        #   pay_de        — 납입일
        #   stock_knd     — 주식종류 (보통주식, 우선주식 …)
        #   isstk_stock_totqy   — 발행주식 총수
        #   isstk_par_vl  — 액면가
        #   isstk_diclo_de — 공시일자
        #   isstk_dlbt_de  — 결제일자
        summary["events"].append({
            "payDate": (r.get("pay_de") or "").strip() or None,
            "stockKind": (r.get("stock_knd") or "").strip() or None,
            "issuedQty": (r.get("isstk_stock_totqy") or "").replace(",", "") or None,
            "parValue": (r.get("isstk_par_vl") or "").replace(",", "") or None,
            "discloseDate": (r.get("isstk_diclo_de") or "").strip() or None,
            "settleDate": (r.get("isstk_dlbt_de") or "").strip() or None,
        })
    return summary


def main():
    require_key()
    if not os.path.exists(INPUT_FILE):
        print(f"ERROR: {INPUT_FILE} 없음 — Phase 1 (build_ipo_history.py) 먼저 실행", file=sys.stderr)
        sys.exit(1)

    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        snap = json.load(f)
    listings = snap.get("listings", [])
    if not listings:
        print("listings 비어있음 — Phase 1 cron이 먼저 돌아야 함", file=sys.stderr)
        sys.exit(0)

    session = make_session()
    corp_map = fetch_corp_code_mapping(session)

    # IPO 종목 중 corp_code 매핑되는 것만 enrich
    targets = [ipo for ipo in listings if corp_map.get(ipo["ticker"])]
    print(f"DART 매핑 가능 종목: {len(targets)}/{len(listings)}", flush=True)

    completed = [0]
    success = [0]
    not_found = [0]

    def enrich(ipo):
        corp_code = corp_map.get(ipo["ticker"])
        bsns_year = int(ipo["listingDate"][:4])
        rows = fetch_irds_sttus(session, corp_code, bsns_year)
        completed[0] += 1
        if completed[0] % 100 == 0:
            print(f"  enriched {completed[0]}/{len(targets)} (success={success[0]}, not_found={not_found[0]})", flush=True)
        if rows is None:
            not_found[0] += 1
            return ipo
        summary = summarize_irds(rows)
        if summary:
            ipo["dart"] = summary
            ipo["dartCorpCode"] = corp_code
            success[0] += 1
        return ipo

    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        list(ex.map(enrich, targets))

    print(f"\n결과: {success[0]}건 enrich 성공, {not_found[0]}건 데이터 없음", flush=True)

    # 출력 (input 그대로 덮어씀)
    snap["lastDartEnrichAt"] = datetime.now(KST).isoformat(timespec="seconds")
    snap["listings"] = listings
    with open(INPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(snap, f, ensure_ascii=False, separators=(",", ":"))
    print(f"Wrote {INPUT_FILE} ({os.path.getsize(INPUT_FILE)/1024:.1f} KB)")


if __name__ == "__main__":
    main()
