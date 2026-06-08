# Iteration 0 — INSERT baseline + breakdown

Goal: Native INSERT +30% over current (~330k rows/sec documented; ~366k measured
fresh this session). 50k mixed rows, best-of-N, M1 Pro, ClickHouse in Docker via
qemu (x86 emulated → server-side insert is artificially slow).

## Breakdown (`bench_insert.py`)

| stage | rows/sec |
|---|---|
| encode only (rows_to_native, no IO) | 645k |
| probe round-trip (LIMIT 0 types) | ~negligible per row |
| **full INSERT (HEADLINE)** | **366k** |

## Decomposition of the full INSERT

| part | rows/sec | µs/row |
|---|---|---|
| encode only | 645k | 1.55 |
| post pre-encoded block (HTTP + server insert) | 636k | 1.57 |
| encode + post (sequential) | ~400k | 2.5 |
| full (truncate + encode + post) | 366–388k | 2.7 |

## Profile of `rows_to_native` (encode)

| tottime | what |
|---|---|
| 2.93s | `b"".join(writer.write(v) for v in values)` — per-value scalar writer |
| 0.77s | `encode_column` |
| 0.71s | `bytes.join` |

Per-column encode speed: numerics 80–200M; **Date 8.5M, String 6.6M, DateTime
2.1M, Array(String) 1.8M** — the non-numeric scalars dominate. GC barely matters
here (bytes aren't cyclically tracked).

## Read

Two independent costs: **client encode** (~1.55µs/row, dominated by per-value
writers for String/Date/DateTime) and the **HTTP+server insert** (~1.57µs/row,
inflated by qemu). They run sequentially, so the full INSERT pays both.

Two levers:
1. Speed up the encode (iter 1) — bulk Date/DateTime/String.
2. Overlap encode with the server insert by streaming blocks (iter 2).
