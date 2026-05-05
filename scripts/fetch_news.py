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
WINDOW_DAYS = 14
TIMEOUT = 15

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
        return builtin
    try:
        with open(KEYWORDS_FILE, encoding="utf-8") as f:
            data = json.load(f)
        defaults = data.get("default") or builtin["default"]
        custom = data.get("custom") or []
        return {"default": list(defaults), "custom": list(custom)}
    except (json.JSONDecodeError, OSError) as e:
        print(f"[warn] could not read keyword file: {e}", file=sys.stderr)
        return builtin


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


def fetch_keyword(session, keyword):
    """Fetch one keyword's RSS feed, return list of article dicts."""
    params = {
        "q": keyword,
        "hl": "ko",
        "gl": "KR",
        "ceid": "KR:ko",
    }
    url = f"{GOOGLE_NEWS_RSS}?{urllib.parse.urlencode(params)}"
    try:
        r = session.get(url, timeout=TIMEOUT)
        r.raise_for_status()
    except requests.RequestException as e:
        print(f"[warn] {keyword}: fetch failed: {e}", file=sys.stderr)
        return []

    try:
        root = ET.fromstring(r.content)
    except ET.ParseError as e:
        print(f"[warn] {keyword}: parse failed: {e}", file=sys.stderr)
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

        if len(items) >= PER_KEYWORD_LIMIT:
            break

    return items


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

    print(f"[info] crawling {len(all_keywords)} keywords")
    session = make_session()

    by_keyword = {}
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futures = {pool.submit(fetch_keyword, session, kw): kw for kw in all_keywords}
        for fut in as_completed(futures):
            kw = futures[fut]
            try:
                items = fut.result()
            except Exception as e:
                print(f"[warn] {kw}: {e}", file=sys.stderr)
                items = []
            by_keyword[kw] = items
            print(f"[info] {kw}: {len(items)} items")

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

    articles = list(flat.values())
    articles.sort(key=lambda x: x.get("published") or "", reverse=True)

    payload = {
        "generated_at": datetime.now(KST).isoformat(timespec="seconds"),
        "window_days": WINDOW_DAYS,
        "total": len(articles),
        "keywords": [
            {"name": kw, "origin": keyword_origin[kw], "count": len(by_keyword.get(kw, []))}
            for kw in all_keywords
        ],
        "articles": articles,
    }

    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    print(f"[info] wrote {OUTPUT_FILE}: {len(articles)} unique articles")


if __name__ == "__main__":
    main()
