#!/usr/bin/env python3
"""Daily Telegram digest of the dashboard's notable updates.

Compiles the day's high-signal items across the data feeds and sends a concise
Korean summary to a Telegram bot. Runs after the feeds refresh (evening KST).

Sections (each skipped when empty):
  💳 신규 메자닌 발행 (오늘 접수된 CB/BW/EB)
  📅 오늘 청약 시작 / 오늘 상장 (IPO 캘린더)
  📑 리서치 목표가 변경 (오늘 리포트 중 상향/하향) + 오늘 리포트 수
  📰 주요 뉴스 (오늘, 상위 몇 건)

Secrets (GitHub Actions):
  TELEGRAM_BOT_TOKEN  — @BotFather 봇 토큰
  TELEGRAM_CHAT_ID    — 알림 받을 chat id (@userinfobot 등으로 확인)

Usage:
  python scripts/telegram_digest.py            # 전송
  python scripts/telegram_digest.py --dry-run  # 토큰 없이 메시지만 출력
"""

import argparse
import html
import json
import os
import sys
from datetime import datetime, timedelta, timezone

import requests

KST = timezone(timedelta(hours=9))
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SITE = "https://loving3645-wq.github.io/my-dashboard"

# How many items to show per section
MAX_MEZZ = 12
MAX_NEWS = 6
MAX_RESEARCH = 12


def load(name):
    try:
        with open(os.path.join(ROOT, "data", name), encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def today_kst():
    return datetime.now(KST).date().isoformat()


def esc(s):
    return html.escape(str(s or ""))


def fmt_amount(won):
    if not won or won <= 0:
        return ""
    if won >= 1e12:
        return f"{won / 1e12:.1f}조"
    if won >= 1e8:
        return f"{round(won / 1e8):,}억"
    return f"{round(won / 1e4):,}만"


def section_mezzanine(day):
    d = load("mezzanine.json")
    if not d:
        return None
    rows = []
    for tk, e in (d.get("mezzanine") or {}).items():
        name = e.get("name") or tk
        for iss in (e.get("issuances") or []):
            if (iss.get("rceptDt") or "")[:10] != day:
                continue
            typ = iss.get("type") or "CB"
            rnd = iss.get("round")
            amt = fmt_amount(iss.get("faceAmount"))
            inv = iss.get("investor")
            tail = " · ".join(x for x in [amt, (f"투자자 {inv}" if inv else "")] if x)
            label = f"{esc(name)} · {esc(typ)}{(' ' + str(rnd) + '회차') if rnd else ''}"
            rows.append(f"{label}{(' · ' + esc(tail)) if tail else ''}")
    if not rows:
        return None
    rows = rows[:MAX_MEZZ]
    body = "\n".join("• " + r for r in rows)
    return f"💳 <b>신규 메자닌 발행</b> ({len(rows)})\n{body}\n<a href=\"{SITE}/mezzanine.html\">메자닌 페이지 →</a>"


def section_ipo(day):
    d = load("ipo-calendar.json")
    if not d:
        return None
    subs, lists = [], []
    for it in (d.get("upcoming") or []):
        nm = esc(it.get("name"))
        if (it.get("subscriptionStart") or "")[:10] == day:
            band = it.get("ipoPriceBand") or (f"{it.get('ipoPrice'):,}" if it.get("ipoPrice") else "")
            uw = it.get("underwriter") or ""
            subs.append(f"{nm}" + (f" · {esc(band)}원" if band else "") + (f" · {esc(uw)}" if uw else ""))
        if (it.get("listingDate") or "")[:10] == day:
            lists.append(nm)
    if not subs and not lists:
        return None
    parts = []
    if subs:
        parts.append("📥 <b>오늘 청약 시작</b>\n" + "\n".join("• " + s for s in subs))
    if lists:
        parts.append("🔔 <b>오늘 상장</b>\n" + "\n".join("• " + s for s in lists))
    return "\n".join(parts) + f"\n<a href=\"{SITE}/ipo-calendar.html\">IPO 캘린더 →</a>"


def section_research(day):
    d = load("research-reports.json")
    if not d:
        return None
    reports = d.get("reports") or []
    todays = [r for r in reports if (r.get("date") or "")[:10] == day]
    if not todays:
        return None
    changes = [r for r in todays if r.get("targetChange") in ("up", "down")]
    lines = []
    for r in changes[:MAX_RESEARCH]:
        arrow = "🔺" if r["targetChange"] == "up" else "🔻"
        tp = f"{r['targetPrice']:,}원" if r.get("targetPrice") else ""
        lines.append(f"{arrow} {esc(r.get('stockName'))} · {esc(r.get('house'))}" + (f" · 목표가 {esc(tp)}" if tp else ""))
    head = f"📑 <b>오늘 리서치</b> {len(todays)}건"
    if lines:
        return head + " · 목표가 변경:\n" + "\n".join(lines) + f"\n<a href=\"{SITE}/research.html\">리서치 페이지 →</a>"
    return head + f"\n<a href=\"{SITE}/research.html\">리서치 페이지 →</a>"


def section_news(day):
    d = load("news.json")
    if not d:
        return None
    arts = d.get("articles") or []
    seen, todays = set(), []
    for a in arts:
        if (a.get("published") or "")[:10] != day:
            continue
        title = (a.get("title") or "").strip()
        key = title.split(" - ")[0]
        if key in seen:
            continue
        seen.add(key)
        todays.append(a)
    if not todays:
        return None
    lines = []
    for a in todays[:MAX_NEWS]:
        title = (a.get("title") or "").split(" - ")[0]
        src = a.get("source") or ""
        url = a.get("url") or ""
        lines.append(f"• <a href=\"{esc(url)}\">{esc(title)}</a>" + (f" <i>{esc(src)}</i>" if src else ""))
    return "📰 <b>주요 뉴스</b>\n" + "\n".join(lines) + f"\n<a href=\"{SITE}/news.html\">뉴스 전체 →</a>"


def build_message():
    day = today_kst()
    nice = datetime.now(KST).strftime("%Y-%m-%d (%a)")
    sections = [
        section_mezzanine(day),
        section_ipo(day),
        section_research(day),
        section_news(day),
    ]
    sections = [s for s in sections if s]
    header = f"📊 <b>지니의 대시보드 · {esc(nice)}</b>"
    if not sections:
        return header + "\n\n오늘은 특이 공시가 없어요. 😌"
    return header + "\n\n" + "\n\n".join(sections)


def send(token, chat_id, text):
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    r = requests.post(url, json={
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }, timeout=20)
    if r.status_code != 200:
        print(f"Telegram API error {r.status_code}: {r.text[:300]}", file=sys.stderr)
        return False
    return True


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="print message, don't send")
    args = ap.parse_args()

    text = build_message()

    if args.dry_run:
        print(text)
        return

    token = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        print("TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID not set — skipping send.",
              file=sys.stderr)
        # Not a failure: the workflow stays green until secrets are added.
        return
    ok = send(token, chat_id, text)
    if not ok:
        sys.exit(1)
    print("Digest sent.")


if __name__ == "__main__":
    main()
