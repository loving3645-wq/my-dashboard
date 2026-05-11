#!/usr/bin/env python3
"""Keyword-based Korean news crawler.

Reads keyword list from data/news-keywords.json (default + user-defined),
queries Google News RSS for each keyword (Korean locale), deduplicates the
combined feed by article URL, and emits data/news.json.

Google News RSS is used instead of scraping Naver because:
  - It is stable (RSS is a public, documented surface)
  - It already aggregates major Korean outlets
  - No API key required

Run on a cron from GitHub Actions.
"""

import html as html_lib
import json
import os
import re
import sys
import time
import urllib.parse
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime

import requests

KST = timezone(timedelta(hours=9))

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
KEYWORDS_FILE = os.path.join(ROOT, "data", "news-keywords.json")
OUTPUT_FILE = os.path.join(ROOT, "data", "news.json")

USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0 Safari/537.36"
)

GOOGLE_NEWS_RSS = "https://news.google.com/rss/search"
WORKERS = 6
PER_KEYWORD_LIMIT = 30
PER_OUTLET_LIMIT = 40
# Cap how many articles any single source can contribute to the final list.
# Prevents aggregators (Investing.com, v.daum.net, 네이트) from dominating.
PER_SOURCE_CAP = 20
WINDOW_DAYS = 14
TIMEOUT = 15

# Direct outlet seeds: Google News indexes these IB/deal-focused outlets, but
# generic keyword queries rarely surface them. Pulling each via a `site:` query
# guarantees baseline coverage.
DEFAULT_OUTLETS = [
    {"name": "더벨", "query": "site:thebell.co.kr"},
    {"name": "인베스트조선", "query": "site:investchosun.com"},
    {"name": "딜사이트", "query": "site:dealsite.co.kr"},
    {"name": "마켓인사이트", "query": "site:hankyung.com 마켓인사이트"},
    {"name": "팍스넷뉴스", "query": "site:paxnetnews.com"},
]

TAG_RE = re.compile(r"<[^>]+>")
WS_RE = re.compile(r"\s+")


def load_keywords():
    """Load default + custom keywords. Falls back to built-in defaults if
    the config file is missing or malformed."""
    builtin = {
        "default": [
            "투자유치", "상장", "IPO", "인수합병", "M&A",
            "유상증자", "무상증자", "자사주", "전환사채",
            "합병", "분할", "신사업", "실적발표",
            "어닝서프라이즈", "매각", "지분인수",
        ],
        "custom": [],
    }
    if not os.path.exists(KEYWORDS_FILE):
        return {**builtin, "outlets": list(DEFAULT_OUTLETS)}
    try:
        with open(KEYWORDS_FILE, encoding="utf-8") as f:
            data = json.load(f)
        defaults = data.get("default") or builtin["default"]
        custom = data.get("custom") or []
        outlets = data.get("outlets")
        if outlets is None:
            outlets = list(DEFAULT_OUTLETS)
        return {
            "default": list(defaults),
            "custom": list(custom),
            "outlets": list(outlets),
        }
    except (json.JSONDecodeError, OSError) as e:
        print(f"[warn] could not read keyword file: {e}", file=sys.stderr)
        return {**builtin, "outlets": list(DEFAULT_OUTLETS)}


def make_session():
    s = requests.Session()
    s.headers.update({
        "User-Agent": USER_AGENT,
        "Accept": "application/rss+xml, application/xml;q=0.9, */*;q=0.8",
        "Accept-Language": "ko-KR,ko;q=0.9,en;q=0.8",
    })
    return s


def strip_html(text):
    if not text:
        return ""
    text = html_lib.unescape(text)
    text = TAG_RE.sub("", text)
    return WS_RE.sub(" ", text).strip()


def parse_pubdate(s):
    if not s:
        return None
    try:
        dt = parsedate_to_datetime(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(KST)
    except (TypeError, ValueError):
        return None


def _fetch_rss_search(session, query, limit, label):
    """Fetch a Google News RSS search query, return list of article dicts."""
    params = {
        "q": query,
        "hl": "ko",
        "gl": "KR",
        "ceid": "KR:ko",
    }
    url = f"{GOOGLE_NEWS_RSS}?{urllib.parse.urlencode(params)}"
    try:
        r = session.get(url, timeout=TIMEOUT)
        r.raise_for_status()
    except requests.RequestException as e:
        print(f"[warn] {label}: fetch failed: {e}", file=sys.stderr)
        return []

    try:
        root = ET.fromstring(r.content)
    except ET.ParseError as e:
        print(f"[warn] {label}: parse failed: {e}", file=sys.stderr)
        return []

    items = []
    cutoff = datetime.now(KST) - timedelta(days=WINDOW_DAYS)
    for item in root.iterfind(".//item"):
        title = strip_html(item.findtext("title"))
        link = (item.findtext("link") or "").strip()
        pub_raw = item.findtext("pubDate")
        pub_dt = parse_pubdate(pub_raw)
        source_el = item.find("source")
        source = source_el.text.strip() if source_el is not None and source_el.text else ""
        description = strip_html(item.findtext("description"))

        if not title or not link:
            continue
        if pub_dt and pub_dt < cutoff:
            continue

        items.append({
            "title": title,
            "url": link,
            "source": source,
            "published": pub_dt.isoformat() if pub_dt else None,
            "summary": description,
        })

        if len(items) >= limit:
            break

    return items


def fetch_keyword(session, keyword):
    return _fetch_rss_search(session, keyword, PER_KEYWORD_LIMIT, keyword)


def fetch_outlet(session, outlet):
    return _fetch_rss_search(session, outlet["query"], PER_OUTLET_LIMIT, f"outlet:{outlet['name']}")


def main():
    keywords_cfg = load_keywords()
    all_keywords = []
    seen = set()
    # Preserve order, dedupe; tag origin so the UI can distinguish.
    keyword_origin = {}
    for kw in keywords_cfg["default"]:
        kw = kw.strip()
        if kw and kw not in seen:
            all_keywords.append(kw)
            keyword_origin[kw] = "default"
            seen.add(kw)
    for kw in keywords_cfg["custom"]:
        kw = kw.strip()
        if kw and kw not in seen:
            all_keywords.append(kw)
            keyword_origin[kw] = "custom"
            seen.add(kw)

    if not all_keywords:
        print("[error] no keywords configured", file=sys.stderr)
        sys.exit(1)

    outlets = [
        o for o in (keywords_cfg.get("outlets") or [])
        if isinstance(o, dict) and o.get("name") and o.get("query")
    ]

    print(f"[info] crawling {len(all_keywords)} keywords + {len(outlets)} outlets")
    session = make_session()

    by_keyword = {}
    by_outlet = {}
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        kw_futures = {pool.submit(fetch_keyword, session, kw): kw for kw in all_keywords}
        outlet_futures = {pool.submit(fetch_outlet, session, o): o for o in outlets}

        for fut in as_completed(kw_futures):
            kw = kw_futures[fut]
            try:
                items = fut.result()
            except Exception as e:
                print(f"[warn] {kw}: {e}", file=sys.stderr)
                items = []
            by_keyword[kw] = items
            print(f"[info] kw {kw}: {len(items)} items")

        for fut in as_completed(outlet_futures):
            o = outlet_futures[fut]
            try:
                items = fut.result()
            except Exception as e:
                print(f"[warn] outlet {o['name']}: {e}", file=sys.stderr)
                items = []
            by_outlet[o["name"]] = items
            print(f"[info] outlet {o['name']}: {len(items)} items")

    # Build a flat, deduplicated article list. Each article carries the list
    # of keywords it matched so a single article tagged by multiple keywords
    # only appears once in the UI.
    flat = {}
    for kw in all_keywords:
        for item in by_keyword.get(kw, []):
            key = item["url"]
            if key in flat:
                if kw not in flat[key]["keywords"]:
                    flat[key]["keywords"].append(kw)
            else:
                entry = dict(item)
                entry["keywords"] = [kw]
                flat[key] = entry

    # Merge outlet articles. They don't add to the keyword chip set — articles
    # surfaced only via outlet seeds get an empty keywords list, so they appear
    # in the unfiltered view but not under any specific keyword filter.
    for o in outlets:
        for item in by_outlet.get(o["name"], []):
            key = item["url"]
            if key in flat:
                continue
            entry = dict(item)
            entry["keywords"] = []
            flat[key] = entry

    # Normalize bare-domain source labels (e.g. "thebell.co.kr") to friendly
    # outlet names ("더벨") so the UI shows consistent labels.
    domain_to_name = {}
    for o in outlets:
        for token in (o.get("query") or "").split():
            if token.lower().startswith("site:"):
                d = token[5:].lower()
                if d.startswith("www."):
                    d = d[4:]
                if d:
                    domain_to_name[d] = o["name"]
    for a in flat.values():
        src = (a.get("source") or "").strip().lower()
        if src.startswith("www."):
            src = src[4:]
        if src in domain_to_name:
            a["source"] = domain_to_name[src]

    articles = list(flat.values())
    articles.sort(key=lambda x: x.get("published") or "", reverse=True)

    # Per-source diversity cap: after sorting newest-first, drop excess articles
    # from sources that have already hit the cap. Keeps the freshest items.
    if PER_SOURCE_CAP > 0:
        seen = {}
        kept = []
        dropped = 0
        for a in articles:
            s = (a.get("source") or "").strip() or "(unknown)"
            if seen.get(s, 0) >= PER_SOURCE_CAP:
                dropped += 1
                continue
            seen[s] = seen.get(s, 0) + 1
            kept.append(a)
        articles = kept
        if dropped:
            print(f"[info] diversity cap={PER_SOURCE_CAP}: dropped {dropped} articles")

    payload = {
        "generated_at": datetime.now(KST).isoformat(timespec="seconds"),
        "window_days": WINDOW_DAYS,
        "total": len(articles),
        "keywords": [
            {"name": kw, "origin": keyword_origin[kw], "count": len(by_keyword.get(kw, []))}
            for kw in all_keywords
        ],
        "outlets": [
            {"name": o["name"], "query": o["query"], "count": len(by_outlet.get(o["name"], []))}
            for o in outlets
        ],
        "per_source_cap": PER_SOURCE_CAP,
        "articles": articles,
    }

    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    print(f"[info] wrote {OUTPUT_FILE}: {len(articles)} unique articles")


if __name__ == "__main__":
    main()
