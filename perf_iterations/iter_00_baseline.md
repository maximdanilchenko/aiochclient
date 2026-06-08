# Iteration 0 — baseline + profile

Goal: Native SELECT 730k → **1,000k rows/sec** (50k mixed rows, best-of-12, M1 Pro, CH 26.5).

## Breakdown (`bench_native.py`)

| stage | rows/sec |
|---|---|
| decode columns only | **1,289k** |
| blocks → Records (count) | 780k |
| fetch (build list) | 790k |
| **fetch + row[:] (HEADLINE)** | **747k** |

## Profile (`--profile`, 20 fetches, tottime)

| tottime | what |
|---|---|
| 0.753s | `native.py:273 <listcomp>` — `[record_from_decoded(v, nm) for v in zip(*columns)]` |
| 0.636s | kqueue control (HTTP IO wait, not CPU) |
| 0.476s | `decode_column` |
| 0.146s | `_execute` |
| 0.048s | Nullable listcomp (`native.py:173`) |
| 0.034s | `array.tolist()` |

## Read

The headline ceiling is the columnar decode at ~1.29M. The row-build step
(transpose `zip(*columns)` + per-row `Record` construction) is the single
biggest CPU cost — **larger than the decode itself**. That is the gap between
1.29M and 747k.

## Next (iteration 1)

Move the transpose + Record construction into a single Cython function
`build_records(columns, name_map)` that builds each row tuple from the column
lists and constructs the `cdef class Record` inline (no Python `zip`, no list
comprehension, no per-row cpdef dispatch). Pure-Python fallback keeps the
current comprehension.
