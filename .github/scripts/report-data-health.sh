#!/usr/bin/env bash
# Turns a health report into exactly one GitHub issue.
#
# Opening a fresh issue every night would train you to ignore them, so
# there is only ever one open issue: it gets edited in place while the
# problem persists, and a new comment (the part that actually sends a
# notification) is posted only when the SET of problems changes. When
# every feed recovers, the issue is closed automatically.
set -euo pipefail

TITLE="🚨 데이터 수집 이상 감지"
LABEL="data-health"
REPORT="health-report.md"

gh label create "$LABEL" \
  --color B60205 \
  --description "데이터 피드 수집 이상" >/dev/null 2>&1 || true

NUM=$(gh issue list --label "$LABEL" --state open --limit 1 \
      --json number --jq '.[0].number // empty')

if [ "$STATUS" = "ok" ]; then
  if [ -n "$NUM" ]; then
    { echo "✅ 모든 피드가 정상으로 돌아왔습니다. 이 이슈를 자동으로 닫습니다."
      echo
      cat "$REPORT"
    } | gh issue comment "$NUM" --body-file -
    gh issue close "$NUM"
    echo "Closed #$NUM — all feeds healthy."
  else
    echo "All feeds healthy, no open issue."
  fi
  exit 0
fi

BODY=$(mktemp)
{
  cat "$REPORT"
  echo
  echo "<!-- fingerprint:${FINGERPRINT} -->"
} > "$BODY"

if [ -z "$NUM" ]; then
  URL=$(gh issue create --title "$TITLE" --label "$LABEL" --body-file "$BODY")
  echo "Opened $URL"
  exit 0
fi

PREV=$(gh issue view "$NUM" --json body --jq '.body' \
       | sed -n 's/.*fingerprint:\([0-9a-f]*\).*/\1/p' | head -1)
gh issue edit "$NUM" --body-file "$BODY" >/dev/null

if [ "$PREV" != "$FINGERPRINT" ]; then
  { echo "문제 상태가 바뀌었습니다."
    echo
    cat "$REPORT"
  } | gh issue comment "$NUM" --body-file -
  echo "Updated #$NUM and posted a comment (problem set changed)."
else
  echo "Updated #$NUM in place (same problem set, no new notification)."
fi
