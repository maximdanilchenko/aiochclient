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

The headline figures below are from the **GitHub-hosted CI runner** (the
`Benchmarks` workflow) — an isolated, modest shared VM (~4 vCPU, Linux). It is
reproducible and representative of commodity hardware; a developer machine is
several times faster across the board (and, importantly, the SELECT gap to
clickhouse-connect narrows there — see below). ClickHouse runs in Docker on the
runner with HTTP `:8123` and native `:9000`. Clients: aiochclient 2.8.0 with the
Cython extension + `ciso8601`; clickhouse-connect, clickhouse-driver, asynch
(latest at run time).

## Results (rows/sec, higher is better)

| Client | Protocol | Async | SELECT (decode) | INSERT |
|:-------|:---------|:-----:|----------------:|-------:|
| clickhouse-connect | HTTP | ✅ | **~750k** | ~415k |
| **aiochclient — Native** | HTTP | ✅ | ~565k | **~415k** |
| clickhouse-driver | native | ❌ (sync) | ~330k | ~290k |
| **aiochclient — RowBinary** | HTTP | ✅ | ~170k | ~170k |
| **aiochclient — TSV** | HTTP | ✅ | ~150k | ~165k |
| asynch | native | ✅ | ~62k | ~95k |

Numbers are indicative (best-of-8; they vary run to run, INSERT more so) and
depend on the data, the ClickHouse version and the machine. They are sensitive to
CPU speed: the columnar decode is single-threaded, and aiochclient builds Python
result objects per row, so it scales with CPU more than clickhouse-connect's
C+numpy decode does. On a fast/idle dev machine all the absolute figures are
several times higher and aiochclient-Native draws level with (or ahead of)
clickhouse-connect on SELECT; on this modest runner connect leads SELECT.

## Takeaways

- **INSERT — aiochclient-Native is on par with clickhouse-connect and ahead of
  the rest.** It encodes columns in bulk and streams the body as Native blocks,
  so the server inserts one block while the client encodes the next.
- **SELECT — clickhouse-connect is fastest here.** It is the official client and
  decodes the columnar format in C on top of numpy, which is largely
  CPU-independent. aiochclient-Native is the next fastest and **by far the
  fastest of aiochclient's own engines** (3–4× its RowBinary/TSV), staying
  dependency-light (**no numpy**); the gap to connect is aiochclient's per-row
  Python objects (`Record`s, tuples, the `int`s from `array.tolist()`) and it
  narrows on faster CPUs.
- **clickhouse-driver** (native protocol, C extensions) is fast but
  **synchronous** — not usable as-is in an asyncio app without a thread pool.
- **RowBinary** is the best *row-oriented* engine (and the one used when streaming
  row-by-row via `iterate`); **TSV** is the zero-Cython baseline.
- **asynch** is the slowest here despite the native protocol — its async row
  materialization appears to be the bottleneck.

### Notes

- The SELECT figures fully materialize every row (`row[:]`), so they measure
  decode + `Record` construction, not lazy passthrough.
- The Native engine's big jump (from ~730k) came from **not running the cyclic
  GC while a fetch allocates its tens of thousands of result objects** — none of
  which are garbage. The collector is disabled only for the synchronous,
  `await`-free decode/build sections and restored afterwards.
- On INSERT, aiochclient-Native encodes columns in bulk (numerics via `array`,
  strings/dates via dedicated encoders) and **streams the body as Native blocks**,
  so the server inserts one block while the client encodes the next. This
  overlap is the bulk of the INSERT win (~+40% over a single-blob insert) and
  puts it ahead of the other clients here.
