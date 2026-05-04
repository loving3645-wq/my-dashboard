# Project conventions

## PR creation policy

이 저장소에서는 **푸시 후 자동으로 PR을 만들고, mergeable 상태면 자동으로 머지까지** 해 주세요. 사용자가 매번 명시적으로 요청하지 않아도 됩니다.

- 작업이 끝나 브랜치를 push 하고 나면, 곧바로 `mcp__github__create_pull_request`로 PR을 생성합니다.
- base 브랜치는 `main`.
- 제목은 커밋 메시지의 첫 줄에 맞춰 짧고 명확하게 (괄호 안의 PR 번호 같은 접미사는 제외).
- 본문은 `## Summary` + `## Test plan` 섹션을 포함합니다.
- PR 생성 직후 `mcp__github__pull_request_read`(method=`get`)로 `mergeable_state`를 확인하고,
  - `clean`이면 `mcp__github__merge_pull_request`로 **squash 머지** (이 저장소의 기존 컨벤션).
  - 충돌/체크 미통과 등 `clean`이 아니면 머지하지 말고 사용자에게 상태를 알립니다.
- 최종적으로 PR URL과 머지 결과(머지됨/대기중/실패)를 메시지 끝에 사용자에게 알립니다.

## 브랜치 작업 규칙

- 사용자가 지정한 feature 브랜치에서만 작업 (예: `claude/...`).
- main에 직접 푸시하지 않음.
