# Iteration 1 — Cython `build_records` (transpose + Record in C)

Replaced the Python `[record_from_decoded(v, nm) for v in zip(*columns)]` in
`blocks_from_native` with a Cython `build_records(columns, name_map)` that builds
each row tuple from the column lists via the C API and fills the cdef Record
inline (no `zip`, no comprehension, no per-row cpdef call).

## Result

| stage | baseline | iter 1 |
|---|---|---|
| fetch + row[:] (HEADLINE) | 747k | **776k** (~+4%) |
| blocks → Records (count) | 780k | 781k |

## Read

Barely moved. In isolation `build_records` does **4.3M rows/sec** and a plain
`list(zip(*cols))` does **9.9M** — so the row-build loop was never the
bottleneck. The cost is elsewhere: allocating ~100k GC-tracked objects (50k
tuples + 50k records) per fetch. Kept the change (it's strictly cheaper and sets
up iter 2) and pivoted to the real cause.
