#!/usr/bin/env python3
"""Health check for every data feed the dashboard reads.

Two independent questions, because either can go wrong on its own:

  1. Is the file on disk fresh and sane?  A workflow can go green while
     writing nothing useful, so freshness is checked against the data,
     not against the run log.
  2. Did the workflow that produces it actually finish?  A run cancelled
     by its job timeout skips the commit step entirely — the file stays
     valid and stale, and nothing anywhere says so. That is exactly how
     the mezzanine feed sat unnoticed for days.

Prints a Markdown report and, under GitHub Actions, writes:
  status=ok|problem      -> the workflow opens/updates/closes an issue
  fingerprint=<sha1>     -> so an unchanged problem set doesn't re-notify

Exit code is always 0: the workflow decides what to do with the result.
"""

import hashlib
import json
import os
import sys
from datetime import datetime, timedelta, timezone

import requests

KST = timezone(timedelta(hours=9))
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA = os.path.join(ROOT, "data")

# maxAgeHours is "how long before staleness is genuinely suspicious":
# the feed's own cron gap, plus the weekend for weekday-only feeds, plus
# a margin for GitHub's scheduled-run delay (observed up to ~3.5h on this
# repo — the 19:00 UTC mezzanine cron regularly starts after 22:00 UTC).
# minCount guards the other failure mode: a run that finishes green but
# writes an empty or truncated file.
FEEDS = [
    {
        "id": "mezzanine",
        "label": "메자닌 발행이력",
        "file": "mezzanine.json",
        "ts": "asOf",
        "maxAgeHours": 34,          # daily cron + delay margin
        "count": "totalIssuances",
        "minCount": 2500,
        "workflow": "fetch-mezzanine.yml",
    },
    {
        "id": "disclosures",
        "label": "DART 공시",
        "file": "disclosures.json",
        "ts": "asOf",
        "maxAgeHours": 80,          # Fri 22:00 KST → Mon 22:00 KST = 72h
        "count": "totalDisclosures",
        "minCount": 500,
        "workflow": "fetch-disclosures.yml",
    },
    {
        "id": "market",
        "label": "국내 시세 스냅샷",
        "file": "market-snapshot.json",
        "ts": "generatedAt",
        "maxAgeHours": 80,
        "count": "tickerCount",
        "minCount": 3000,
        "workflow": "snapshot-market.yml",
    },
    {
        "id": "news",
        "label": "뉴스",
        "file": "news.json",
        "ts": "generated_at",
        "maxAgeHours": 18,          # 12h overnight gap + delay margin
        "count": "total",
        "minCount": 100,
        "workflow": "fetch-news.yml",
    },
    {
        "id": "research",
        "label": "리서치 리포트",
        "file": "research-reports.json",
        "ts": "generatedAt",
        "maxAgeHours": 80,
        "count": "count",
        "minCount": 30,
        "workflow": "research-reports.yml",
    },
    {
        "id": "ipo-calendar",
        "label": "IPO 캘린더",
        "file": "ipo-calendar.json",
        "ts": "generatedAt",
        "maxAgeHours": 34,          # daily cron + delay margin
        "count": "count",
        "minCount": 1,
        "workflow": "ipo-calendar.yml",
    },
    {
        "id": "ipo-history",
        "label": "IPO 히스토리",
        "file": "ipo-history.json",
        "ts": "generatedAt",
        "maxAgeHours": 200,         # weekly
        "count": "count",
        "minCount": 900,
        "workflow": "ipo-history.yml",
    },
]

# data/us-market-snapshot.json and data/mezzanine-extra.json are produced
# by manual scripts with no cron, so staleness there is expected — they
# are deliberately not monitored.


def _parse_ts(raw):
    if not raw or not isinstance(raw, str):
        return None
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=KST)
    return dt


def check_feed(feed):
    """Returns (problems, info) — problems is a list of (kind, message)."""
    problems = []
    path = os.path.join(DATA, feed["file"])
    info = {"age": None, "count": None, "asOf": None}

    if not os.path.exists(path):
        return [("missing", f"`data/{feed['file']}` 파일 없음")], info
    try:
        with open(path, encoding="utf-8") as f:
            payload = json.load(f)
    except ValueError as e:
        return [("corrupt", f"`data/{feed['file']}` JSON 파싱 실패 — {e}")], info

    ts = _parse_ts(payload.get(feed["ts"]))
    info["asOf"] = payload.get(feed["ts"])
    if ts is None:
        problems.append(
            ("no_timestamp", f"`{feed['ts']}` 필드를 읽을 수 없음")
        )
    else:
        age_h = (datetime.now(KST) - ts).total_seconds() / 3600
        info["age"] = age_h
        if age_h > feed["maxAgeHours"]:
            problems.append((
                "stale",
                f"{age_h:.0f}시간째 갱신 없음 "
                f"(허용 {feed['maxAgeHours']}h, 기준 {info['asOf']})",
            ))

    count = payload.get(feed["count"])
    info["count"] = count
    if not isinstance(count, int):
        problems.append(
            ("no_count", f"`{feed['count']}` 필드를 읽을 수 없음")
        )
    elif count < feed["minCount"]:
        problems.append((
            "short",
            f"{feed['count']}={count:,} — 최소 기대치 {feed['minCount']:,} 미만",
        ))

    # Feeds that record their own run outcome (mezzanine does) get an
    # extra check: a run can finish and commit while knowing it only got
    # part of the data.
    last = payload.get("lastRun")
    if isinstance(last, dict) and last.get("complete") is False:
        detail = ", ".join(
            f"{k}={last[k]}" for k in ("apiFailed", "budgetSkipped")
            if last.get(k)
        )
        problems.append((
            "partial",
            f"마지막 수집이 불완전하게 끝남 ({detail or 'complete=false'})",
        ))
    return problems, info


def latest_run(session, repo, workflow_file):
    """Most recent completed run of a workflow, or None if unknown."""
    url = (
        f"https://api.github.com/repos/{repo}/actions/workflows/"
        f"{workflow_file}/runs"
    )
    try:
        r = session.get(
            url, params={"per_page": 5, "status": "completed"}, timeout=20
        )
        if r.status_code != 200:
            return None
        runs = r.json().get("workflow_runs") or []
    except (requests.RequestException, ValueError):
        return None
    return runs[0] if runs else None


def main():
    repo = os.environ.get("GITHUB_REPOSITORY", "")
    token = os.environ.get("GITHUB_TOKEN", "")
    session = requests.Session()
    session.headers.update({
        "Accept": "application/vnd.github+json",
        "User-Agent": "my-dashboard/health-check",
    })
    if token:
        session.headers["Authorization"] = f"Bearer {token}"

    rows = []
    problems = {}          # feed id -> list of (kind, message)

    for feed in FEEDS:
        feed_problems, info = check_feed(feed)

        run = latest_run(session, repo, feed["workflow"]) if repo else None
        run_note = "—"
        if run:
            concl = run.get("conclusion") or "?"
            when = (run.get("updated_at") or "")[:16].replace("T", " ")
            run_note = f"{concl} ({when}Z)"
            if concl in ("failure", "cancelled", "timed_out", "startup_failure"):
                feed_problems.append((
                    f"run_{concl}",
                    f"최근 워크플로 실행이 `{concl}` "
                    f"([로그]({run.get('html_url')}))",
                ))

        if feed_problems:
            problems[feed["id"]] = feed_problems

        age = info["age"]
        rows.append({
            "label": feed["label"],
            "ok": not feed_problems,
            "age": "—" if age is None else f"{age:.0f}h",
            "count": "—" if info["count"] is None else f"{info['count']:,}",
            "run": run_note,
            "problems": feed_problems,
        })

    ok = not problems
    lines = []
    lines.append(
        f"**점검 시각**: {datetime.now(KST).strftime('%Y-%m-%d %H:%M')} KST"
    )
    lines.append("")
    lines.append("| 피드 | 상태 | 경과 | 건수 | 최근 실행 |")
    lines.append("|---|---|---|---|---|")
    for r in rows:
        lines.append(
            f"| {r['label']} | {'✅' if r['ok'] else '🚨'} | "
            f"{r['age']} | {r['count']} | {r['run']} |"
        )

    if problems:
        lines.append("")
        lines.append("### 문제 상세")
        for feed in FEEDS:
            if feed["id"] not in problems:
                continue
            lines.append("")
            lines.append(f"**{feed['label']}** (`{feed['workflow']}`)")
            for _, msg in problems[feed["id"]]:
                lines.append(f"- {msg}")
        lines.append("")
        lines.append(
            "수동 재실행: Actions 탭에서 해당 워크플로 → `Run workflow`."
        )

    report = "\n".join(lines)
    print(report)

    fingerprint = hashlib.sha1(
        json.dumps(
            {k: sorted(kind for kind, _ in v) for k, v in problems.items()},
            sort_keys=True,
        ).encode()
    ).hexdigest()[:12]

    out_path = os.environ.get("GITHUB_OUTPUT")
    if out_path:
        with open(out_path, "a", encoding="utf-8") as f:
            f.write(f"status={'ok' if ok else 'problem'}\n")
            f.write(f"fingerprint={fingerprint}\n")
            f.write(f"problem_count={len(problems)}\n")
    with open(os.path.join(ROOT, "health-report.md"), "w", encoding="utf-8") as f:
        f.write(report + "\n")

    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a", encoding="utf-8") as f:
            f.write(report + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
