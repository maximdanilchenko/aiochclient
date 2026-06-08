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
- **Client versions**: aiochclient 2.7.0 + the RowBinary and Native engines
  (with the Cython extension built and `ciso8601` 2.3.3); clickhouse-connect
  1.1.1; clickhouse-driver 0.2.10; asynch 0.3.1; aiohttp 3.14.0.

## Results (rows/sec, higher is better)

| Client | Protocol | Async | SELECT (decode) | INSERT |
|:-------|:---------|:-----:|----------------:|-------:|
| **aiochclient — Native** | HTTP | ✅ | **~1,000k** | ~330k |
| **aiochclient — RowBinary** | HTTP | ✅ | ~370k | ~320k |
| **aiochclient — TSV** | HTTP | ✅ | ~305k | ~245k |
| clickhouse-connect | HTTP | ✅ | ~820k | **~450k** |
| asynch | native | ✅ | ~108k | ~165k |
| clickhouse-driver | native | ❌ (sync) | ~440k | ~370k |

Numbers are indicative (best-of-8 on an otherwise-idle machine; they vary
±10–20% run to run, INSERT more so) and depend on the data, the ClickHouse
version and the machine. They are also sensitive to background CPU load — on a
busy machine the absolute figures fall (the columnar decode is single-threaded),
though the ordering holds.

## Takeaways

- **aiochclient's Native engine is the fastest SELECT here** (~1M rows/sec),
  ahead of clickhouse-connect, while keeping aiochclient's small,
  dependency-light footprint (**no numpy**). It decodes ClickHouse's columnar
  `Native` format whole-column-at-once — fixed-width numerics straight through
  the stdlib `array` module, strings/dates in the compiled Cursor, then one
  transpose to rows — and, crucially, suppresses the cyclic garbage collector
  while it allocates the result objects (see below).
- **clickhouse-connect** is the official client and decodes the columnar binary
  format in C on top of numpy; it is the next fastest on SELECT and the fastest
  on INSERT.
- **clickhouse-driver** (native protocol, C extensions) is fast but
  **synchronous** — not usable as-is in an asyncio app without a thread pool.
- **RowBinary** remains the best *row-oriented* engine (and the one used when
  streaming row-by-row via `iterate`); **TSV** is the zero-Cython baseline.
- **asynch** is the slowest on SELECT here despite the native protocol — its
  async row materialization appears to be the bottleneck.

### Notes

- The SELECT figures fully materialize every row (`row[:]`), so they measure
  decode + `Record` construction, not lazy passthrough.
- The Native engine's big jump (from ~730k) came from **not running the cyclic
  GC while a fetch allocates its tens of thousands of result objects** — none of
  which are garbage. The collector is disabled only for the synchronous,
  `await`-free decode/build sections and restored afterwards.
- INSERT throughput is closer across clients: aiochclient-Native encodes numeric
  columns in bulk via `array`, but variable-width / mixed columns still encode
  per value, so its INSERT lands near RowBinary and clickhouse-driver rather than
  ahead of them.
