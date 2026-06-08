# Iteration 4 — load sensitivity, GC-safety tests, docs

Wrap-up after reaching the 1M goal in iter 3. Two things: confirm the win is
real (not a measurement fluke) and lock it down with safety tests + docs.

## Measurement is load-sensitive (be honest about it)

Re-running later, the absolute numbers had collapsed: focused headline ~350–390k,
`decode columns only` ~460k (vs ~1.33M earlier) — and clickhouse-connect was
*also* not at its earlier peak. Cause: background CPU load on this (corporate)
laptop — `jamf protect` security scanning (~23% CPU) and the Dockerised
ClickHouse under qemu (~29%), load average ~3–4. The single-threaded columnar
decode gets squeezed (likely onto efficiency cores), so the absolute throughput
roughly tracks how busy the machine is.

Controlled A/B in one process (so load is identical for both arms):

| machine state | gc-naive (pre-opt) | gc-managed (this change) | gc fully disabled |
|---|---|---|---|
| idle (earlier) | ~693k | ~1,040k | ~1,240k |
| loaded (now) | ~322k | ~379k | ~384k |

Two facts hold regardless of load:
1. **The optimization always helps and never hurts** — gc-managed beats gc-naive
   (1.15× under heavy load, ~1.8× idle) and **matches the gc-fully-disabled
   ceiling**, i.e. it captures essentially all of the available GC win safely.
2. The headline of ~1.0M rows/sec is real on an otherwise-idle machine
   (reproduced across several focused runs at ~1.04M and the official
   `benchmarks_vs_libs.py` at 1,000,175).

Docs (README, BENCHMARK_RESULTS.md) quote the idle-machine figures with an
explicit "sensitive to background load" caveat.

## GC-safety tests (added to `TestNative`)

The optimization disables the cyclic collector in synchronous sections, so the
risk is mismanaging that global flag. Three tests pin the contract:

- `test_fetch_restores_gc_state` — GC enabled before a fetch ⇒ enabled after.
- `test_fetch_preserves_caller_gc_disabled` — if the caller disabled GC, a fetch
  must **not** turn it back on (we only re-enable what we disabled).
- `test_gc_state_restored_after_decode_error` — an error raised mid-decode still
  restores the flag (the `try/finally` covers every path, incl. the retry).

All green (12 = 3 × http×config). Full suite 912 passed on cython, pure fallback
green.

## Status

Optimization shipped as-is. The riskier "disable GC across the whole fetch
(spanning awaits)" path — which reached ~1.24M idle — was deliberately **not**
taken: it would leave GC off across `await` points, a footgun under concurrency.
