# Iteration 3 — suppress GC during column decode too ✅ GOAL MET

## Change

Extended the iter-2 idea to the decode side: `decode_column_with_prefix`
(per-column, called once inside each synchronous `reader.parse`) now disables
the cyclic GC around the prefix read + column body decode, restoring it after.
Like `build_records`, this is an `await`-free synchronous section, so it is safe
and contained; GC still runs between columns/blocks and during HTTP IO.

Decoding allocates each column's whole value list (and every string/object
element) at once — the same mass-allocation that trips generational GC.

## Result (`bench_native.py`, best of 12)

| stage | baseline | iter 2 | iter 3 |
|---|---|---|---|
| decode columns only | 1289k | ~1180k | **1326k** |
| blocks → Records (count) | 780k | 998k | **1098k** |
| fetch (build list) | 790k | 1010k | **1074k** |
| **fetch + row[:] (HEADLINE)** | 747k | ~1011k | **~1040k** (1049 / 1039 / 1039) |

## Official `benchmarks_vs_libs.py`

```
aiochclient (HTTP, Native)    select 1000175 rows/sec | insert  411763 rows/sec
clickhouse-connect (HTTP)     select  785671 rows/sec | insert  365199 rows/sec
clickhouse-driver (native)    select  449009 rows/sec | insert  408229 rows/sec
```

**Native SELECT ≥ 1,000,000 rows/sec — goal reached**, and now the fastest
client in the comparison (ahead of clickhouse-connect), still numpy-free.

## Correctness

Full suite 909 passed on cython, pure-Python fallback green. GC state is
captured/restored with try/finally on every path (including the NeedMoreData
retry), and every disabled region is synchronous (no `await`), so no other
coroutine ever runs with GC off.

## Summary of what moved the needle

The whole 730k → 1.0M gain came from **not letting the cyclic garbage collector
run while a fetch allocates its tens of thousands of result objects** — none of
which are garbage. The Cython transpose (iter 1) was a near-no-op on its own;
the row-build loop was never the bottleneck.

## Possible follow-ups (not done here)

- The same GC-suppression could help the RowBinary row driver, but it builds
  rows one-by-one across `await` points, so it would need per-row toggling
  (overhead) or an unsafe await-spanning disable — needs care.
- Update README / BENCHMARK_RESULTS.md headline numbers if this lands.
