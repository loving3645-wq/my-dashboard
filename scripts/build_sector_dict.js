#!/usr/bin/env node
/* =====================================================================
 * data/sector-dict.json 생성기
 *
 * tickers-full.js(상장사 2,613개 + KSIC 업종) 와 sectors.js(23섹터 분류기)를
 * 합쳐, 상장사 전체를 12개 폴더 섹터로 미리 계산해 둔 사전을 만든다.
 *
 * 쓰는 쪽:
 *   1) Google Apps Script — 메일로 들어온 리포트 PDF를 드라이브 섹터 폴더로
 *      분류할 때. 파일명에 섹터 단어가 없는 게 대부분이라(리포트 제목은
 *      마케팅 문구다) 종목명 사전이 없으면 분류가 41%까지 떨어진다.
 *   2) 향후 요약 파이프라인 — 같은 사전을 써야 드라이브 폴더와 대시보드
 *      섹터가 어긋나지 않는다.
 *
 * 실행: node scripts/build_sector_dict.js
 * ===================================================================== */

const fs = require('fs');
const path = require('path');

const ROOT = path.dirname(__dirname);
global.window = global;
require(path.join(ROOT, 'tickers-full.js'));
require(path.join(ROOT, 'themes.js'));
require(path.join(ROOT, 'sectors.js'));

// 드라이브 폴더 12개. 순서가 곧 인덱스이므로 바꾸면 사전을 다시 생성해야 한다.
const SECTOR_KEYS = [
  '반도체·IT',        // 0
  '바이오·제약',      // 1
  '2차전지·자동차',   // 2
  '화학·소재·철강',   // 3
  '정유·에너지',      // 4
  '조선·기계·방산',   // 5
  '건설·건자재',      // 6
  '통신·미디어·게임', // 7
  '금융·지주·보험',   // 8
  '유통·소비재',      // 9
  '운송·물류',        // 10
  '시황·전략',        // 11
];

// sectors.js 의 23섹터 → 위 12개 인덱스
const TO12 = {
  '반도체': 0, 'IT·전기전자': 0, '소프트웨어·인터넷': 0,
  '바이오·제약': 1,
  '2차전지': 2, '자동차': 2,
  '화학·소재': 3, '철강·비철금속': 3,
  '정유·에너지': 4,
  '조선': 5, '기계·로봇': 5, '방산·우주': 5,
  '건설·건자재': 6,
  '통신': 7, '미디어·엔터': 7, '게임': 7,
  '은행·증권·지주': 8, '보험': 8,
  '유통·소비재': 9, '음식료': 9, '화장품·패션': 9,
  '운송·물류': 10,
  '전략·기타': 11,
};

// 네이버 리서치의 업종 리포트 카테고리 → 12섹터.
// 업종 리포트는 종목코드가 없어서 이 사전이 유일한 단서다.
const INDUSTRY_NAMES = [
  'IT', '전기전자', '반도체', '인터넷포탈', '소프트웨어', '게임', '미디어',
  '자동차', '조선', '기계', '운송', '항공운송', '해운', '석유화학', '화학',
  '철강금속', '에너지', '유틸리티', '제약', '바이오', '건설', '건자재',
  '통신', '은행', '증권', '지주회사', '보험', '유통', '음식료', '화장품',
  '섬유의복', '기타',
];

const byCode = {};
const byName = {};
for (const [code, reg] of Object.entries(window.TICKER_REGISTRY)) {
  const idx = TO12[window.SECTORS.ofTicker(code) || '전략·기타'];
  if (idx === undefined) continue;
  byCode[code] = idx;
  // 동명이인은 사실상 없지만, 있으면 먼저 등록된 쪽을 남긴다.
  if (byName[reg.name] === undefined) byName[reg.name] = idx;
}

const byIndustry = {};
for (const name of INDUSTRY_NAMES) {
  byIndustry[name] = TO12[window.SECTORS.ofIndustry(name)];
}

const out = {
  generatedAt: new Date().toISOString().slice(0, 10),
  source: 'tickers-full.js + sectors.js',
  sectors: SECTOR_KEYS,
  byCode,
  byName,
  byIndustry,
};

const target = path.join(ROOT, 'data', 'sector-dict.json');
fs.writeFileSync(target, JSON.stringify(out));

const dist = {};
Object.values(byCode).forEach(i => { dist[SECTOR_KEYS[i]] = (dist[SECTOR_KEYS[i]] || 0) + 1; });
console.log(`Wrote ${target}`);
console.log(`  종목코드 ${Object.keys(byCode).length} / 종목명 ${Object.keys(byName).length} / 업종명 ${Object.keys(byIndustry).length}`);
console.log(`  크기 ${(fs.statSync(target).size / 1024).toFixed(1)} KB`);
Object.entries(dist).sort((a, b) => b[1] - a[1])
  .forEach(([k, v]) => console.log(`  ${k.padEnd(18)} ${v}`));
