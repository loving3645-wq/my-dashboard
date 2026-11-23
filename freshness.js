/* freshness.js — 각 페이지 상단에 "마지막 갱신 N시간/일 전" 배지를 그린다.
 *
 * 사용법(페이지 헤더에 한 줄):
 *   <span class="fresh-badge" data-fresh-src="data/news.json"
 *         data-fresh-field="generated_at" data-fresh-warn="8" data-fresh-stale="24"></span>
 *   <script src="freshness.js" defer></script>
 *
 * 타임스탬프는 JSON 맨 앞 키라서, 큰 파일도 앞 2KB만 Range로 읽어 가볍다.
 * warn 시간 이상이면 🟡, stale 시간 이상이면 🔴(갱신 지연). 기본 36h/72h.
 */
(function () {
  var CSS = [
    '.fresh-badge{display:inline-flex;align-items:center;gap:5px;font-family:var(--mono,ui-monospace,monospace);',
    'font-size:11px;line-height:1;padding:3px 9px;border-radius:999px;border:1px solid var(--line,#333);',
    'background:var(--surface,#14171e);color:var(--ink-soft,#8b93a2);letter-spacing:.02em;white-space:nowrap;',
    'vertical-align:middle;cursor:default;}',
    '.fresh-badge.ok{color:var(--green,#35d0a5);border-color:var(--green,#35d0a5);}',
    '.fresh-badge.warn{color:var(--gold,#ffb03a);border-color:var(--gold,#ffb03a);}',
    '.fresh-badge.stale{color:var(--red,#ff5d6a);border-color:var(--red,#ff5d6a);font-weight:600;}',
    '.fresh-badge.loading,.fresh-badge.unknown{color:var(--ink-faint,#5b6473);border-style:dashed;}'
  ].join('');

  function injectCSS() {
    if (document.getElementById('fresh-badge-css')) return;
    var st = document.createElement('style');
    st.id = 'fresh-badge-css';
    st.textContent = CSS;
    document.head.appendChild(st);
  }

  function relKo(ms) {
    var m = Math.round(ms / 60000);
    if (m < 1) return '방금';
    if (m < 60) return m + '분 전';
    var h = Math.round(m / 60);
    if (h < 48) return h + '시간 전';
    var d = Math.round(h / 24);
    return d + '일 전';
  }

  // JSON 앞부분만 읽어 "field":"..." 값을 꺼낸다. Range 미지원이면 스트림을 조기 종료.
  async function headTimestamp(url, field) {
    var sep = url.indexOf('?') >= 0 ? '&' : '?';
    var full = url + sep + 't=' + Date.now();
    var text = '';
    try {
      var res = await fetch(full, { headers: { Range: 'bytes=0-2047' }, cache: 'no-store' });
      if (!res.ok && res.status !== 206) return null;
      if (res.body && res.body.getReader) {
        var reader = res.body.getReader();
        var dec = new TextDecoder();
        while (text.length < 4096) {
          var chunk = await reader.read();
          if (chunk.done) break;
          text += dec.decode(chunk.value, { stream: true });
        }
        try { reader.cancel(); } catch (e) {}
      } else {
        text = (await res.text()).slice(0, 4096);
      }
    } catch (e) {
      return null;
    }
    var m = text.match(new RegExp('"' + field + '"\\s*:\\s*"([^"]+)"'));
    return m ? m[1] : null;
  }

  async function render(el) {
    var url = el.getAttribute('data-fresh-src');
    var field = el.getAttribute('data-fresh-field') || 'generatedAt';
    var warnH = parseFloat(el.getAttribute('data-fresh-warn') || '36');
    var staleH = parseFloat(el.getAttribute('data-fresh-stale') || '72');
    if (!url) return;
    el.className = 'fresh-badge loading';
    el.textContent = '갱신 확인 중…';
    var iso = await headTimestamp(url, field);
    if (!iso) {
      el.className = 'fresh-badge unknown';
      el.textContent = '갱신 시각 불명';
      return;
    }
    var when = new Date(iso);
    var age = Date.now() - when.getTime();
    var ageH = age / 3600000;
    var cls = 'ok', dot = '🟢', suffix = '';
    if (ageH >= staleH) { cls = 'stale'; dot = '🔴'; suffix = ' · 갱신 지연'; }
    else if (ageH >= warnH) { cls = 'warn'; dot = '🟡'; }
    el.className = 'fresh-badge ' + cls;
    el.title = when.toLocaleString('ko-KR') + ' 기준';
    el.textContent = dot + ' ' + relKo(age) + ' 갱신' + suffix;
  }

  function init() {
    injectCSS();
    var els = document.querySelectorAll('[data-fresh-src]');
    for (var i = 0; i < els.length; i++) render(els[i]);
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
