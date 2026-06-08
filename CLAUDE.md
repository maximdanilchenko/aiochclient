# CLAUDE.md

Guidance for working in this repository.

## What this is

`aiochclient` — an async HTTP(S) ClickHouse client for Python. It talks to ClickHouse
over its HTTP interface (default `http://localhost:8123/`), converts types in both
directions, streams results, and decodes rows lazily. The public API is a single class,
`ChClient`, plus the `Record` row object and the `ChClientError` exception.

It has three interchangeable wire engines, selected on `ChClient`:
- **TSV** (default) — text `TSVWithNamesAndTypes`, lazy per-row decoding.
- **RowBinary** (`binary=True`) — the row-oriented binary format; faster, best for
  streaming row-by-row via `iterate`.
- **Native** (`native=True`) — ClickHouse's column-oriented binary format; fastest for
  bulk SELECT/INSERT, decoded whole-column-at-once without numpy.

Package is published to PyPI as `aiochclient`. Current version: `2.8.0` (see `setup.py`).

## Architecture

The package lives in `aiochclient/`:

- **`client.py`** — `ChClient`, the public entry point. Holds connection params/headers,
  builds query strings, picks the right ClickHouse `FORMAT`, and orchestrates fetch/insert.
  Constructor flags: `binary` / `native` (engine), `insert_block_size` (Native INSERT block
  size; 0 = single atomic block). Public methods: `execute`, `fetch`, `fetchrow`, `fetchval`,
  `iterate`, `insert_file`, `is_alive`, `close`. `fetchone`/`cursor` are deprecated aliases.
- **`http_clients/`** — pluggable HTTP backend behind `HttpClientABC` (`abc.py`).
  `choose_http_client()` auto-detects whether the passed session is an `aiohttp.ClientSession`
  or `httpx.AsyncClient` and returns the matching impl (`aiohttp.py` / `httpx.py`). Both
  implement `get` / `post_return_lines` (streaming line generator, TSV/JSON) /
  `post_return_bytes` (streaming byte-chunk generator, RowBinary/Native) / `post_no_return`
  (also accepts an async-generator body for streamed Native INSERT) / `close`. Non-200
  responses raise `ChClientError`.
- **`types.py`** — pure-Python type conversion (the TSV engine and the per-value binary
  read/write primitives). `BaseType` subclasses (one per ClickHouse type) implement `convert`
  (CH bytes → Python), `unconvert` (Python → CH text), and the RowBinary `read`/`write`.
  `CH_TYPES_MAPPING` maps CH type names → classes; `PY_TYPES_MAPPING` maps Python types →
  unconvert fns. Containers parse recursively via `what_py_type`. Also holds the binary
  `Cursor` (with bulk column reads) and `read_column`.
- **`_types.pyx`** — Cython port of `types.py` for a speedup. **Keep in sync with `types.py`**
  — both must implement the same behavior. Also hosts the compiled `Record`,
  `build_records` (column→row transpose), `write_string_column`, and `record_new`. Everything
  imports from `_types` (compiled) and falls back to `types` if the C extension isn't built.
- **`binary.py`** — the **RowBinary** engine. `BinaryReader` (buffered O(n) reader over the
  byte-chunk stream), `RowBinaryFabric` / `rows_from_binary` (read), `rows_to_binary` (write),
  `fetch_column_types` / `fetch_column_header` (LIMIT 0 type probe for INSERTs). Imports the
  `Cursor`, `read_row`, `read_column`, `what_py_type` from `_types`/`types`.
- **`native.py`** — the **Native** (columnar) engine. Read: `decode_column` (whole-column
  decode; bulk numerics via stdlib `array`, strings/dates via the Cursor, `_read_prefix` for
  LowCardinality state), `blocks_from_native` (yields a `Record` list per block, GC suppressed
  during the synchronous decode/build), `rows_from_native`. Write: `encode_column` (bulk
  column encoders), `rows_to_native` (one block) and `rows_to_native_stream` (async generator
  of blocks for streamed INSERT). `_wire_type` simplifies LowCardinality/Nested for the wire.
- **`records.py`** — `Record` (lazy `Mapping`, access by name or index, decodes on first
  access). Re-exports the compiled `Record` + `record_from_decoded` from `_types` when built,
  else defines the pure version. `RecordsFabric` (TSV header rows), `FromJsonFabric` (JSON).
- **`sql.py`** — patches `sqlparse`'s lexer/keywords so ClickHouse-specific syntax
  (`FORMAT`, `DESCRIBE`, `SHOW`, `EXISTS`) parses correctly. Imported for its side effects;
  `client.py` uses `sqlparse` to detect statement type and format.
- **`exceptions.py`** — `ChClientError`, plus `NeedMoreData` (internal, drives the binary
  reader's refill loop).

### Request flow

`_execute()` in `client.py` is the core. It substitutes `params` into the query string,
parses the statement (`_parse_squery`) to decide whether to fetch and which `FORMAT` to
append (`Native` / `RowBinaryWithNamesAndTypes` / `TSVWithNamesAndTypes`, or `JSONEachRow`
when `json=True`). Fetches: the columnar/binary engines stream byte chunks via
`post_return_bytes` and drive `rows_from_native` / `rows_from_binary`; TSV/JSON stream lines
via `post_return_lines`. `fetch` has a fast path that consumes Native results block-by-block.
INSERTs: Native encodes columnar blocks (streamed via `rows_to_native_stream`), RowBinary
encodes via `rows_to_binary` (both after a LIMIT 0 type probe), TSV via `rows2ch`, JSON via
`json2ch`. All public fetch methods delegate to `_execute`.

## Build / test / dev commands

```
make format                  # isort + black
make check_format            # verify formatting (black --target-version py310)
make build_cython            # compile _types.pyx in place (python setup.py build_ext --inplace)
make test                    # installs dev reqs, starts a ClickHouse docker container, runs pytest
make docker-clickhouse       # start ClickHouse on :8123 (container name "cs")
make html_types              # cython annotated HTML for _types.pyx
```

Tests live in a single file, `tests.py`, and need a live ClickHouse at `localhost:8123`
(`make docker-clickhouse` provides one). Tests are async (`pytest-asyncio`, `asyncio_mode = auto`).
`benchmarks.py` benchmarks throughput.

The CI matrix (`.github/workflows/tests.yml`) runs Python 3.10–3.13 × four dependency sets
(plain / ciso8601 / cython / cython+ciso8601), defined in `dev-requirements/`. The minimum
supported Python is 3.10 (`python_requires` in `setup.py`).

## Conventions

- Formatting: **black** with `skip-string-normalization = true` (single quotes are fine)
  and **isort**. Run `make format` before committing.
- Optional native acceleration is the norm: any change to type conversion (or the compiled
  helpers like `Cursor`, `read_column`, `read_row`, `Record`, `build_records`,
  `write_string_column`) must be made in **both** `types.py` and `_types.pyx`, and any pure
  fallback in `binary.py` / `native.py` kept in sync. Build with `make build_cython`; run the
  suite both with the extension built and with it removed.
- Speedup extras are optional installs: `ciso8601` (fast datetime parsing), `aiodns` +
  `faust-cchardet` (aiohttp), and the compiled Cython extension. Code must work without any
  of them — always provide a pure-Python fallback path (see the `try/except ImportError`
  blocks).
- Install targets: `aiochclient[aiohttp]`, `aiochclient[httpx]`, and their `*-speedups` variants.

## Gotchas

- Query `params` use `str.format`, so literal `{`/`}` in SQL (e.g. cluster macros) must be
  escaped as `{{`/`}}`.
- The TSV streaming backend asserts the response ends on a line boundary (`assert not buffer`).
- **Native engine specifics**: `decode=False` (raw bytes) is TSV-only; the Native format
  carries no per-column timezone, so a tz-aware `DateTime`/`DateTime64` comes back as naive
  UTC. A multi-block native INSERT is **not atomic** on a client-side encoding error (blocks
  already streamed are committed) — `insert_block_size=0` forces a single atomic block. The
  GC is disabled inside the synchronous decode/build sections of a fetch (mass allocation
  otherwise trips the cyclic collector); it is always restored via `try/finally`, and those
  sections contain no `await`.
- Both binary engines look up the target column types via a `LIMIT 0
  RowBinaryWithNamesAndTypes` probe before an INSERT, since encoding is type-specific.
- `perf_iterations/` and `perf_insert/` are performance-experiment logs; they are pruned from
  the sdist/wheel (`MANIFEST.in`) and are not part of the package.
