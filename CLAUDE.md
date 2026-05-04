# Project conventions

## PR creation policy

이 저장소에서는 **푸시 후 자동으로 PR을 만들어 주세요**. 사용자가 매번 명시적으로 요청하지 않아도 됩니다.

- 작업이 끝나 브랜치를 push 하고 나면, 곧바로 `mcp__github__create_pull_request`로 PR을 생성합니다.
- base 브랜치는 `main`.
- 제목은 커밋 메시지의 첫 줄에 맞춰 짧고 명확하게 (괄호 안의 PR 번호 같은 접미사는 제외).
- 본문은 `## Summary` + `## Test plan` 섹션을 포함합니다.
- PR URL을 메시지 끝에 사용자에게 알려줍니다.

## 브랜치 작업 규칙

- 사용자가 지정한 feature 브랜치에서만 작업 (예: `claude/...`).
- main에 직접 푸시하지 않음.
