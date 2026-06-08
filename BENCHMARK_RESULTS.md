# aiochclient vs. other Python ClickHouse clients

Throughput comparison against the most popular Python ClickHouse clients on a
real-world-ish workload. Reproduce with `benchmarks_vs_libs.py`.

## Workload

- **Table** (`Memory` engine), mixed types similar to a typical events row:
  `id UInt32, value Int64, score Float64, name String, created Date,
  ts DateTime, tags Array(String), opt Nullable(Int32)`.
- **SELECT**: `SELECT *` of **50,000** rows, fully materialized into Python
  objects (list of row tuples). For aiochclient this forces decoding of every
  field (`row[:]`); the other clients already return materialized rows.
- **INSERT**: bulk-insert the same 50,000 rows (table truncated before each run).
- Throughput = rows / best wall-clock over **8 sequential runs**, single
  connection, **no async parallelism** (so this measures serialization /
  deserialization cost, not IO concurrency).
- Correctness was checked: every client returns the same 50,000 rows with
  identical decoded values.

## Environment

- **Machine**: Apple M1 Pro, macOS (Darwin 23.6), CPython 3.11.
- **ClickHouse**: 26.5 in Docker, local. HTTP on `:8123`, native TCP on `:9000`.
- **Client versions**: aiochclient 2.7.0 + the RowBinary engine (with the Cython
  extension built and `ciso8601` 2.3.3); clickhouse-connect 1.1.1;
  clickhouse-driver 0.2.10; asynch 0.3.1; aiohttp 3.14.0.

## Results (rows/sec, higher is better)

| Client | Protocol | Async | SELECT (decode) | INSERT |
|:-------|:---------|:-----:|----------------:|-------:|
| **aiochclient — TSV** | HTTP | ✅ | ~265k | ~242k |
| **aiochclient — RowBinary** | HTTP | ✅ | ~280k | ~310k |
| clickhouse-connect | HTTP | ✅ | **~770k** | **~450k** |
| asynch | native | ✅ | ~108k | ~158k |
| clickhouse-driver | native | ❌ (sync) | ~455k | ~360k |

Numbers are indicative (best-of-8; they vary ±10–20% run to run, INSERT more so)
and depend on the data, the ClickHouse version and the machine.

## Takeaways

- **clickhouse-connect is the fastest** here, by a wide margin on SELECT. It is
  the official client and decodes ClickHouse's **columnar** binary format in a C
  extension, transposing to rows in bulk — much cheaper than any row-by-row
  approach. If raw throughput is the only goal, it's the one to beat.
- **clickhouse-driver** (native protocol, C extensions) is the next fastest, but
  it is **synchronous** — not usable as-is in an asyncio app without a thread
  pool.
- **aiochclient's RowBinary engine** is the fastest of the lightweight,
  row-oriented **async** options: it beats its own TSV path on both directions
  and is faster than `asynch` (the async native driver) here, while keeping the
  small, dependency-light, streaming, lazy-decoding HTTP design.
- **asynch** is the slowest on SELECT in this test despite using the native
  protocol — its async row materialization appears to be the bottleneck.

### Where aiochclient could close the gap

The remaining SELECT gap to clickhouse-connect is **columnar decoding**:
aiochclient (and the native row drivers) decode row-by-row, while
clickhouse-connect decodes whole columns at once in C. A columnar read path
(e.g. ClickHouse's `Native` format) would be the architectural change needed to
compete with it on raw read throughput — a possible future direction beyond the
current RowBinary work.
