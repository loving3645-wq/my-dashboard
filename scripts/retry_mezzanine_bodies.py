#!/usr/bin/env python3
"""Retry disclosure-body parsing for issuances that previously came up
empty.

The main fetch workflow scans ~2,600 tickers + 3 endpoints + body fetches
per live issuance — 15-25 minutes per run. When DART rate-limits some
body fetches mid-run, those return None and (by design) are NOT cached
so they get retried on the next full run. But waiting for the next full
run is slow.

This retry-only entrypoint iterates issuances already in
data/mezzanine.json that:
  - are not yet matured,
  - have a rceptNo,
  - have no putStart,
  - have no cache entry yet.

That's the exact set of "we tried, body fetch failed, want to retry"
issuances. Run takes 1-5 minutes depending on volume. Trigger via the
retry-mezzanine.yml workflow_dispatch button.
"""
import json
import os
import sys
from datetime import date

# Reuse all parsing / fetch helpers from the main fetch module.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import fetch_mezzanine_history as fmh

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MEZZ_FILE = os.path.join(ROOT, "data", "mezzanine.json")


def main():
    if not fmh.DART_KEY:
        print("ERROR: OPENDART_KEY env var not set", file=sys.stderr)
        sys.exit(1)

    cache_loaded = fmh.load_body_cache()
    print(f"Loaded body-parse cache: {cache_loaded:,} entries", flush=True)

    with open(MEZZ_FILE, encoding="utf-8") as f:
        mezz = json.load(f)

    today = date.today().isoformat()
    candidates = []
    for ticker, e in mezz["mezzanine"].items():
        for iss in e["issuances"]:
            if iss.get("putStart"):
                continue
            if iss.get("maturityDate") and iss["maturityDate"] < today:
                continue
            rcept_no = iss.get("rceptNo")
            if not rcept_no:
                continue
            if fmh._cache_get(rcept_no) is not None:
                continue
            candidates.append((ticker, iss, rcept_no))

    print(f"Retrying {len(candidates):,} issuances without put data",
          flush=True)
    if not candidates:
        print("Nothing to do.")
        return

    sess = fmh.make_session()
    new_put = 0
    new_call = 0

    for i, (ticker, iss, rcept_no) in enumerate(candidates, 1):
        if i % 50 == 0:
            print(
                f"  {i}/{len(candidates)} "
                f"(new put={new_put} call={new_call})",
                flush=True,
            )
        body = fmh.fetch_disclosure_body(sess, rcept_no)
        if not body:
            fmh._record_parse("no_body")
            continue
        ps, pe, pct, outcome = fmh.parse_put_schedule(
            body, iss.get("issueDate"), iss.get("maturityDate"),
            fmh.PUT_KEYWORD_RE,
            fmh.PUT_START_LABEL_PATTERNS, fmh.PUT_END_LABEL_PATTERNS,
            stop_re=fmh.PUT_PARSE_STOP_RE,
        )
        fmh._record_parse(outcome)
        cs, ce, cpct, _ = fmh.parse_put_schedule(
            body, iss.get("issueDate"), iss.get("maturityDate"),
            fmh.CALL_KEYWORD_RE,
            fmh.PUT_START_LABEL_PATTERNS, fmh.PUT_END_LABEL_PATTERNS,
            stop_re=fmh.CALL_PARSE_STOP_RE,
        )
        outcome_key = outcome if isinstance(outcome, str) else outcome[0]
        fmh._cache_put(rcept_no, {
            "putStart": ps, "putEnd": pe, "putPct": pct,
            "callStart": cs, "callEnd": ce, "callPct": cpct,
            "outcome": outcome_key,
        })
        if ps:
            iss["putStart"] = ps
            iss["putSource"] = "body"
            new_put += 1
        if pe:
            iss["putEnd"] = pe
        if pct:
            iss["putPct"] = pct
        if cs:
            iss["callStart"] = cs
            new_call += 1
        if ce:
            iss["callEnd"] = ce
        if cpct:
            iss["callPct"] = cpct

    print(
        f"\nDone. Newly extracted: put={new_put}, call={new_call}",
        flush=True,
    )

    with open(MEZZ_FILE, "w", encoding="utf-8") as f:
        json.dump(mezz, f, ensure_ascii=False, separators=(",", ":"))
    if fmh.save_body_cache():
        print(
            f"Cache size: {len(fmh._BODY_CACHE):,} entries",
            flush=True,
        )
    print(f"Wrote {MEZZ_FILE}", flush=True)

    # Surface failure stats so the user can decide whether to re-run.
    total_fail = sum(fmh._API_FAILURES.values())
    if total_fail:
        print(
            f"\nAPI failures after retries: total={total_fail} "
            f"ratelimit={fmh._API_FAILURES['ratelimit']} "
            f"http_error={fmh._API_FAILURES['http_error']} "
            f"transient={fmh._API_FAILURES['transient']} "
            f"network={fmh._API_FAILURES['network']}",
            flush=True,
        )


if __name__ == "__main__":
    main()
