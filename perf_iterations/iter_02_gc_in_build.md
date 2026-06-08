# Iteration 2 — suppress cyclic GC during `build_records` ⭐

## Diagnosis

Disabling the cyclic garbage collector globally for the whole fetch took the
headline from ~693k to **1,239,762 rows/sec** in a controlled test. Cause:
materializing ~100k GC-tracked containers (50k row tuples + 50k records) per
fetch repeatedly trips generational GC mid-build, even though none of those
objects are garbage (they are all about to be returned).

## Change

`build_records` (Cython + pure fallback) now disables GC for its loop and
restores the previous state afterwards. This is a tight, synchronous,
`await`-free section, so no other coroutine runs while GC is off — it is safe
and contained (GC still runs normally between blocks / during IO).

## Result

| stage | baseline | iter 2 |
|---|---|---|
| decode columns only | 1289k | ~1180k |
| blocks → Records (count) | 780k | **998k** |
| fetch (build list) | 790k | **1010k** |
| **fetch + row[:] (HEADLINE)** | 747k | **~1011k** |

Official `benchmarks_vs_libs.py`: aiochclient Native SELECT **910k** — now ahead
of clickhouse-connect (~831k) on the same workload.

## Read

Goal essentially reached (hovering 0.91M–1.01M across runs, ±5–10% variance).
The residual gap to the global-GC-off ceiling (~1.24M) is GC during the *decode*
phase (column-list allocation). Iter 3 will suppress that too — only in the
synchronous decode section, to stay safe.
