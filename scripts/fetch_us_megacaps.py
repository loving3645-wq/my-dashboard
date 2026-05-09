#!/usr/bin/env python3
"""US NASDAQ mega-cap snapshot (market cap >= $500B).

Fetches daily OHLC for a curated list of NASDAQ-listed common stocks from
Yahoo Finance's public chart endpoint (no API key, browser UA required),
computes 1D/1W/1M/3M/6M/1Y returns, pulls recent news headlines as the
"why it moved" hint, and writes data/us-megacaps.json.

Frontend (`us-market.html`) reads the JSON directly. Market cap is computed
client-server-side using a hardcoded shares-outstanding lookup (mega-caps
have stable share counts; refresh the constants here on splits/buybacks).

Cron: weekday afternoon US time, GitHub Actions.
"""

import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

import requests

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUTPUT_FILE = os.path.join(ROOT, "data", "us-megacaps.json")

USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0 Safari/537.36"
)

CHART_URL = (
    "https://query1.finance.yahoo.com/v8/finance/chart/"
    "{symbol}?range=1y&interval=1d&includePrePost=false"
)
NEWS_URL = (
    "https://query1.finance.yahoo.com/v1/finance/search"
    "?q={symbol}&newsCount=8&quotesCount=0&enableFuzzyQuery=false"
)

WORKERS = 12
RETRY = 2
MIN_MARKET_CAP_USD = 500_000_000_000  # $500B

# Curated NASDAQ-listed common stocks that are at or near the $500B threshold.
# `shares` is shares outstanding (in raw count, not millions). Numbers are
# rounded; refresh on splits/major buybacks. Sources: latest 10-Q/10-K.
# `overview` is a 1-2 sentence Korean blurb about the business.
UNIVERSE = [
    {
        "symbol": "NVDA",
        "name": "NVIDIA Corporation",
        "sector": "Semiconductors",
        "shares": 24_400_000_000,
        "overview": "AI 가속기(GPU) 시장의 사실상 독점 사업자. 데이터센터·게이밍·자율주행용 GPU와 CUDA 생태계로 매출 대부분을 창출.",
    },
    {
        "symbol": "AAPL",
        "name": "Apple Inc.",
        "sector": "Consumer Electronics",
        "shares": 14_840_000_000,
        "overview": "아이폰을 중심으로 맥·아이패드·웨어러블 + 서비스(앱스토어·아이클라우드)까지 통합한 글로벌 1위 하드웨어 브랜드.",
    },
    {
        "symbol": "MSFT",
        "name": "Microsoft Corporation",
        "sector": "Software",
        "shares": 7_430_000_000,
        "overview": "윈도·오피스 + Azure 클라우드 + OpenAI 파트너십을 통한 AI 인프라까지 보유한 종합 SW 거인.",
    },
    {
        "symbol": "GOOGL",
        "name": "Alphabet Inc. Class A",
        "sector": "Internet Services",
        "shares": 6_070_000_000,
        "overview": "구글 검색·광고가 캐시카우. 유튜브, Google Cloud, Waymo, Gemini AI 모델까지 보유. (의결권 있음)",
    },
    {
        "symbol": "GOOG",
        "name": "Alphabet Inc. Class C",
        "sector": "Internet Services",
        "shares": 6_080_000_000,
        "overview": "Alphabet의 의결권 없는 보통주. 비즈니스는 GOOGL과 동일.",
    },
    {
        "symbol": "AMZN",
        "name": "Amazon.com, Inc.",
        "sector": "E-commerce / Cloud",
        "shares": 10_620_000_000,
        "overview": "북미·EU 1위 이커머스 + AWS(글로벌 1위 퍼블릭 클라우드) + 광고. 영업이익은 AWS·광고에서 대부분 발생.",
    },
    {
        "symbol": "META",
        "name": "Meta Platforms, Inc.",
        "sector": "Social Media / Advertising",
        "shares": 2_540_000_000,
        "overview": "페이스북·인스타그램·왓츠앱 광고로 매출 거의 전부. Reality Labs(메타버스/AI 인프라)에 대규모 CAPEX 투자 중.",
    },
    {
        "symbol": "TSLA",
        "name": "Tesla, Inc.",
        "sector": "EV / Energy",
        "shares": 3_200_000_000,
        "overview": "전기차 글로벌 톱티어. FSD(자율주행 SW)·로보택시·에너지(파워월·메가팩) 사업 확장 중. 변동성 매우 큼.",
    },
    {
        "symbol": "AVGO",
        "name": "Broadcom Inc.",
        "sector": "Semiconductors / Software",
        "shares": 4_700_000_000,
        "overview": "커스텀 ASIC(하이퍼스케일러용 AI 칩) + 네트워크 칩 + VMware 인수로 SW 매출도 대형. 엔비디아 다음의 AI 수혜주.",
    },
    {
        "symbol": "COST",
        "name": "Costco Wholesale Corporation",
        "sector": "Retail",
        "shares": 443_000_000,
        "overview": "회원제 창고형 할인점. 매출 성장과 마진 안정성, 멤버십 갱신율(>90%)이 장기 컴파운더의 전형.",
    },
    {
        "symbol": "NFLX",
        "name": "Netflix, Inc.",
        "sector": "Streaming",
        "shares": 426_000_000,
        "overview": "글로벌 1위 스트리밍. 광고요금제·계정공유 단속·스포츠 라이브로 ARM(가입자당 매출) 성장 가속화.",
    },
    {
        "symbol": "ASML",
        "name": "ASML Holding N.V.",
        "sector": "Semiconductor Equipment",
        "shares": 393_000_000,
        "overview": "EUV 노광장비 글로벌 독점. 첨단 반도체 공정 장비 매출 비중 큼. 미·중 수출 규제 영향 직접 받음.",
    },
    {
        "symbol": "AMD",
        "name": "Advanced Micro Devices, Inc.",
        "sector": "Semiconductors",
        "shares": 1_620_000_000,
        "overview": "엔비디아의 가장 강력한 AI GPU 도전자(MI300/350 시리즈). 데이터센터 CPU(EPYC)·게이밍·임베디드까지.",
    },
    {
        "symbol": "PEP",
        "name": "PepsiCo, Inc.",
        "sector": "Consumer Staples",
        "shares": 1_370_000_000,
        "overview": "탄산음료 + 프리토레이(스낵). 디펜시브 + 배당귀족.",
    },
    {
        "symbol": "TMUS",
        "name": "T-Mobile US, Inc.",
        "sector": "Telecom",
        "shares": 1_160_000_000,
        "overview": "미국 1위 이동통신사(가입자 기준). 5G 인프라 우위와 자사주 매입으로 EPS 성장 견조.",
    },
    {
        "symbol": "CSCO",
        "name": "Cisco Systems, Inc.",
        "sector": "Networking",
        "shares": 3_960_000_000,
        "overview": "네트워크 장비 글로벌 1위. Splunk 인수로 보안·옵저버빌리티 SW 매출 비중 확대 중.",
    },
    {
        "symbol": "INTU",
        "name": "Intuit Inc.",
        "sector": "Software",
        "shares": 280_000_000,
        "overview": "TurboTax(세무) + QuickBooks(중소기업 회계) + Credit Karma. 미국 SMB SaaS 사실상 독점.",
    },
    {
        "symbol": "ADBE",
        "name": "Adobe Inc.",
        "sector": "Software",
        "shares": 425_000_000,
        "overview": "Photoshop·Premiere 등 크리에이티브 SW 구독(Creative Cloud) + Document Cloud + Experience Cloud.",
    },
    {
        "symbol": "AZN",
        "name": "AstraZeneca PLC (ADR)",
        "sector": "Pharmaceuticals",
        "shares": 3_100_000_000,
        "overview": "영국 본사 글로벌 제약사. 종양학·심혈관·호흡기 파이프라인 강력. NASDAQ ADR 상장.",
    },
    {
        "symbol": "LIN",
        "name": "Linde plc",
        "sector": "Industrial Gases",
        "shares": 478_000_000,
        "overview": "글로벌 1위 산업가스. 반도체·헬스케어·에너지 공정에 필수. 디펜시브 컴파운더.",
    },
    {
        "symbol": "ARM",
        "name": "Arm Holdings plc (ADR)",
        "sector": "Semiconductor IP",
        "shares": 1_050_000_000,
        "overview": "모바일 CPU 아키텍처 IP 라이선스. 삼성·애플·퀄컴 등 거의 모든 모바일 SoC 기반. AI/데이터센터 침투 진행 중.",
    },
    {
        "symbol": "QCOM",
        "name": "Qualcomm Incorporated",
        "sector": "Semiconductors",
        "shares": 1_110_000_000,
        "overview": "스마트폰 AP/모뎀(스냅드래곤) 글로벌 1위. 자동차·IoT·PC로 다각화 중.",
    },
    {
        "symbol": "AMAT",
        "name": "Applied Materials, Inc.",
        "sector": "Semiconductor Equipment",
        "shares": 810_000_000,
        "overview": "글로벌 반도체 장비 빅3. 식각·증착·CMP 공정 장비 보유. 첨단 공정·HBM 사이클 수혜.",
    },
    {
        "symbol": "BKNG",
        "name": "Booking Holdings Inc.",
        "sector": "Online Travel",
        "shares": 33_000_000,
        "overview": "Booking.com·Priceline·Agoda·Kayak. 글로벌 1위 OTA. 자사주 매입 공격적.",
    },
]


def make_session():
    s = requests.Session()
    s.headers.update(
        {
            "User-Agent": USER_AGENT,
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
        }
    )
    return s


def get_with_retry(session, url, retries=RETRY):
    last = None
    for _ in range(retries + 1):
        try:
            r = session.get(url, timeout=15)
            if r.status_code == 200 and r.text:
                return r
            last = f"HTTP {r.status_code}"
        except Exception as e:
            last = str(e)
        time.sleep(0.6)
    raise RuntimeError(last or "unknown error")


def fetch_chart(session, symbol):
    """Return dict with price, returns, and a sparkline (252 daily closes)."""
    r = get_with_retry(session, CHART_URL.format(symbol=symbol))
    j = r.json()
    res = (j.get("chart") or {}).get("result") or []
    if not res:
        raise RuntimeError("no chart result")
    res = res[0]
    meta = res.get("meta") or {}
    ts = res.get("timestamp") or []
    quote = ((res.get("indicators") or {}).get("quote") or [{}])[0]
    closes_raw = quote.get("close") or []
    # Drop None entries (Yahoo sometimes leaves a trailing null on the active day)
    closes = []
    timestamps = []
    for t, c in zip(ts, closes_raw):
        if c is not None and c > 0:
            closes.append(float(c))
            timestamps.append(int(t))
    if len(closes) < 5:
        raise RuntimeError("insufficient history")

    cur = float(meta.get("regularMarketPrice") or closes[-1])
    prev_close = float(meta.get("chartPreviousClose") or closes[-2])

    def lookback_pct(days):
        # Trading days: 5/wk, so approximate calendar days -> trading idx
        # Use direct index from end. closes oldest -> newest.
        if len(closes) <= days:
            return None
        past = closes[-1 - days]
        if past <= 0:
            return None
        return round((cur / past - 1) * 100, 2)

    return {
        "currency": meta.get("currency") or "USD",
        "exchange": meta.get("fullExchangeName") or meta.get("exchangeName") or "",
        "price": round(cur, 2),
        "prevClose": round(prev_close, 2),
        "change1d": round(cur - prev_close, 2),
        "r1d": round((cur / prev_close - 1) * 100, 2) if prev_close > 0 else None,
        "r1w": lookback_pct(5),
        "r1m": lookback_pct(21),
        "r3m": lookback_pct(63),
        "r6m": lookback_pct(126),
        "r1y": lookback_pct(252),
        "high52w": meta.get("fiftyTwoWeekHigh"),
        "low52w": meta.get("fiftyTwoWeekLow"),
        # Full daily-close history for the last ~1y, for chart rendering.
        # Frontend slices by selected period.
        "history": [
            {"t": t, "c": round(c, 2)} for t, c in zip(timestamps, closes)
        ],
    }


def fetch_news(session, symbol):
    """Return up to ~6 recent news items: title, publisher, link, time."""
    try:
        r = get_with_retry(session, NEWS_URL.format(symbol=symbol))
        j = r.json()
    except Exception:
        return []
    items = []
    for n in (j.get("news") or [])[:8]:
        title = (n.get("title") or "").strip()
        if not title:
            continue
        items.append(
            {
                "title": title,
                "publisher": n.get("publisher") or "",
                "link": n.get("link") or "",
                "time": n.get("providerPublishTime") or 0,
            }
        )
    return items


def fetch_one(session, entry):
    sym = entry["symbol"]
    chart = fetch_chart(session, sym)
    news = fetch_news(session, sym)
    market_cap = round(chart["price"] * entry["shares"])
    return {
        "symbol": sym,
        "name": entry["name"],
        "sector": entry["sector"],
        "overview": entry["overview"],
        "sharesOutstanding": entry["shares"],
        "marketCap": market_cap,
        **chart,
        "news": news,
    }


def main():
    session = make_session()
    out = {}
    errors = []

    print(f"Fetching {len(UNIVERSE)} US tickers...", flush=True)
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futs = {ex.submit(fetch_one, session, e): e["symbol"] for e in UNIVERSE}
        for fut in as_completed(futs):
            sym = futs[fut]
            try:
                row = fut.result()
                out[sym] = row
                tag = "INCLUDED" if row["marketCap"] >= MIN_MARKET_CAP_USD else "below"
                print(
                    f"  {sym:6s} ${row['price']:>8.2f}  "
                    f"mcap=${row['marketCap']/1e9:>7.1f}B  ({tag})",
                    flush=True,
                )
            except Exception as e:
                errors.append((sym, str(e)))
                print(f"  {sym:6s} ERROR: {e}", flush=True)

    # Filter to >= $500B and sort by market cap desc
    qualified = [
        v for v in out.values() if v["marketCap"] >= MIN_MARKET_CAP_USD
    ]
    qualified.sort(key=lambda x: x["marketCap"], reverse=True)

    now_utc = datetime.now(timezone.utc).replace(microsecond=0)
    payload = {
        "generatedAtUtc": now_utc.isoformat().replace("+00:00", "Z"),
        "version": 1,
        "minMarketCapUsd": MIN_MARKET_CAP_USD,
        "universeSize": len(UNIVERSE),
        "fetched": len(out),
        "qualified": len(qualified),
        "errors": [{"symbol": s, "error": e} for s, e in errors],
        "tickers": qualified,
    }

    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, separators=(",", ":"))

    size_kb = os.path.getsize(OUTPUT_FILE) / 1024
    print(
        f"\nWrote {OUTPUT_FILE} ({size_kb:.1f} KB) — "
        f"{len(qualified)}/{len(out)} qualified (>= $500B)",
        flush=True,
    )

    # Hard fail if too many fetches errored
    if errors and len(errors) > len(UNIVERSE) // 2:
        print(f"ERROR: {len(errors)} fetch errors, refusing to clobber", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
