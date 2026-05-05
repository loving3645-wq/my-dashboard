# 대시보드 단계별 개발 계획

## 목표

운용역 1인이 **종목 코드 하나 입력**하면 시세·재무·메자닌·지분구조·IR·뉴스를 한 화면에서 보고, 필요 시 Word 리포트로 떨어뜨리는 도구.

## 원칙

- **기존 아키텍처 유지**: Python 스크래퍼 → `data/*.json` → 정적 HTML. Streamlit/서버 추가하지 않음.
- **토큰 비용 0원 기본**: LLM 호출은 IR/뉴스 요약 단계에서만, 캐싱 필수.
- **GitHub Actions 크론으로 자동 적재**, 프론트는 JSON만 읽음.

## 3계층 구조

```
[1] 수집 (LLM 없음, 매일 크론)
    KIS/Naver 시세, OpenDART 재무·공시, 뉴스 → data/*.json

[2] 분석 (LLM 없음, 페이지 로딩 시 JS)
    이자보상배율, CB 희석률, 풋옵션 캘린더, 잔량 계산

[3] 요약 (LLM, 선택적)
    IR/뉴스 본문 자연어 요약 — 결과 캐시
```

## Phase 1 — 종목 단일 화면 재무 보강

기존 `stock-deep-dive.html` 위에 얹는다.

- **수집 스크립트**: `scripts/fetch_dart_financials.py`
  - OpenDART API → 3개년 손익/재무/현금흐름
  - 종목별 `data/financials/{ticker}.json` 산출
- **프론트**: `stock-deep-dive.html`에 재무 요약 섹션 추가
  - 매출/영업이익/순이익 3개년 추이, 부채비율, 이자보상배율
- **워크플로**: `.github/workflows/fetch-financials.yml` (주 1회)

**산출물 검증**: 종목코드 입력 시 시세 + 시총 + 3개년 재무가 한 화면에 뜨면 통과.

## Phase 2 — 메자닌 발행이력 추출기 (핵심 차별화)

이 프로젝트의 진짜 가치. 정형 API 없음 → 직접 파서 작성.

- **수집 스크립트**: `scripts/build_mezzanine_history.py`
  - DART 「주요사항보고서(전환사채권발행결정)」, 「전환청구권행사」, 「발행공시」 수집
  - 회사별 미상환 잔량 계산 로직
  - `data/mezzanine/{ticker}.json` (발행건별 잔량·전환가·풋옵션 일정)
- **프론트**: `stock-deep-dive.html`에 메자닌 섹션
  - 발행이력 타임라인, 잔량/희석률, 풋옵션 도래 캘린더

**리스크**: 본문 형식이 회사마다 다름. 파서 회귀 테스트 필수.

## Phase 3 — 지분구조·담보·IR 요약·뉴스

여기서 처음으로 LLM 호출.

- **지분구조/담보**: 분기보고서 본문 파싱 → `data/ownership/{ticker}.json`
- **IR 요약**: Claude API 호출, 공시 ID별 캐시 (`data/summaries/{disclosure_id}.json`)
- **뉴스**: 안정성 위해 유료 뉴스 API 권장 (네이버 비공식은 차단 빈번)

**비용 가드**: 같은 공시 두 번 요약 안 함. 한 종목 분석 LLM 비용 1,000원 이하.

## Phase 4 — Word 리포트

`python-docx`로 종목별 한 페이지 리포트. Phase 1~3 데이터 합쳐 출력.

- `scripts/export_report.py {ticker}` → `reports/{ticker}_{date}.docx`

## 기술 스택 (확정)

| 영역 | 선택 |
|---|---|
| 수집 | Python 3.11+, `requests`, `lxml` |
| 저장 | `data/*.json` (커밋), 대용량 시 SQLite 검토 |
| 프론트 | 기존 정적 HTML + Vanilla JS |
| 자동화 | GitHub Actions cron |
| LLM | Claude API (Phase 3 진입 시) |
| 리포트 | `python-docx` |

## 사전 준비

- [ ] OpenDART API 키 발급 (https://opendart.fss.or.kr, 무료, 1분)
- [ ] GitHub Secrets에 `OPENDART_KEY` 등록 (Phase 1 진입 직전)
- [ ] Claude API 키 (Phase 3 진입 직전)

## 진행 순서

1. 본 계획 머지
2. Phase 1 PR (재무 수집 + UI 섹션)
3. Phase 2 PR (메자닌 — 가장 시간 소요)
4. Phase 3 PR (요약/뉴스)
5. Phase 4 PR (Word)

각 Phase는 독립 PR로 끊는다. 메인은 항상 동작하는 상태 유지.
