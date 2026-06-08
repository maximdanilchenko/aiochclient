# aiochclient

[![PyPI version](https://badge.fury.io/py/aiochclient.svg)](https://badge.fury.io/py/aiochclient)
[![Tests](https://github.com/maximdanilchenko/aiochclient/actions/workflows/tests.yml/badge.svg)](https://github.com/maximdanilchenko/aiochclient/actions/workflows/tests.yml)
[![Documentation Status](https://readthedocs.org/projects/aiochclient/badge/?version=latest)](https://aiochclient.readthedocs.io/en/latest/?badge=latest)
[![codecov](https://codecov.io/gh/maximdanilchenko/aiochclient/branch/master/graph/badge.svg)](https://codecov.io/gh/maximdanilchenko/aiochclient)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/ambv/black)


An async http(s) ClickHouse client for python 3.10+ supporting type
conversion in both directions, streaming, lazy decoding on select queries, and a
fully typed interface.

## Table of Contents

- [Installation](#installation)
- [Quick Start](#quick-start)
- [Binary engines: RowBinary and Native](#binary-engines-rowbinary-and-native)
- [Documentation](#documentation)
- [Type Conversion](#type-conversion)
- [Connection Pool Settings](#connection-pool-settings)
- [Notes on Speed](#notes-on-speed)

## Installation

You can use it with either
[aiohttp](https://github.com/aio-libs/aiohttp) or
[httpx](https://github.com/encode/httpx) http connectors.

To use with `aiohttp` install it with command:

```
> pip install aiochclient[aiohttp]
```

Or `aiochclient[aiohttp-speedups]` to install with extra speedups.

To use with `httpx` install it with command:

```
> pip install aiochclient[httpx]
```

Or `aiochclient[httpx-speedups]` to install with extra speedups.

Installing with `[*-speedups]` adds the following:

- [faust-cchardet](https://pypi.org/project/faust-cchardet/) for `aiohttp` speedup
- [aiodns](https://pypi.org/project/aiodns/) for `aiohttp` speedup
- [ciso8601](https://github.com/closeio/ciso8601) for ultra-fast datetime
  parsing while decoding data from ClickHouse for `aiohttp` and `httpx`.

Additionally the installation process attempts to use Cython for a speed boost
(roughly 30% faster).

## Quick Start

### Connecting to ClickHouse

`aiochclient` needs `aiohttp.ClientSession` or `httpx.AsyncClient` to connect to ClickHouse:

```python
from aiochclient import ChClient
from aiohttp import ClientSession


async def main():
    async with ClientSession() as s:
        client = ChClient(s)
        assert await client.is_alive()  # returns True if connection is Ok

```

### Querying the database

```python
await client.execute(
    "CREATE TABLE t (a UInt8, b Tuple(Date, Nullable(Float32))) ENGINE = Memory"
)
```

For INSERT queries you can pass values as `*args`. Values should be
iterables:

```python
await client.execute(
    "INSERT INTO t VALUES",
    (1, (dt.date(2018, 9, 7), None)),
    (2, (dt.date(2018, 9, 8), 3.14)),
)
```

For fetching all rows at once use the
[`fetch`](https://aiochclient.readthedocs.io/en/latest/api.html#aiochclient.ChClient.fetch)
method:

```python
all_rows = await client.fetch("SELECT * FROM t")
```

For fetching first row from result use the
[`fetchrow`](https://aiochclient.readthedocs.io/en/latest/api.html#aiochclient.ChClient.fetchrow)
method:

```python
row = await client.fetchrow("SELECT * FROM t WHERE a=1")

assert row[0] == 1
assert row["b"] == (dt.date(2018, 9, 7), None)
```

You can also use
[`fetchval`](https://aiochclient.readthedocs.io/en/latest/api.html#aiochclient.ChClient.fetchval)
method, which returns first value of the first row from query result:

```python
val = await client.fetchval("SELECT b FROM t WHERE a=2")

assert val == (dt.date(2018, 9, 8), 3.14)
```

With async iteration on the query results stream you can fetch multiple
rows without loading them all into memory at once:

```python
async for row in client.iterate(
        "SELECT number, number*2 FROM system.numbers LIMIT 10000"
):
    assert row[0] * 2 == row[1]
```

Use `fetch`/`fetchrow`/`fetchval`/`iterate` for SELECT queries and `execute` or
any of last for INSERT and all another queries.

### Working with query results

All fetch queries return rows as lightweight, memory efficient objects. _Before
v`1.0.0` rows were only returned as tuples._ All rows have a full mapping interface, where you can
get fields by names or indexes:

```python
row = await client.fetchrow("SELECT a, b FROM t WHERE a=1")

assert row["a"] == 1
assert row[0] == 1
assert row[:] == (1, (dt.date(2018, 9, 8), 3.14))
assert list(row.keys()) == ["a", "b"]
assert list(row.values()) == [1, (dt.date(2018, 9, 8), 3.14)]
```

### Binary engines: RowBinary and Native

By default results are encoded/decoded through ClickHouse's text (TSV) format.
Two faster binary engines are available, each opt-in via a single flag; both use
the Cython extension when it's built and fall back to pure Python otherwise.

**`native=True`** — ClickHouse's column-oriented `Native` format, the fastest
engine for SELECT. Whole columns are decoded at once (fixed-width numerics
straight through the stdlib `array` module, strings/dates in the compiled
Cursor), which rivals the C-extension clients while keeping aiochclient
dependency-light — **no numpy**. INSERTs are encoded column-by-column into a
single Native block.

```python
client = ChClient(session, native=True)
await client.execute("INSERT INTO t VALUES", (1, "a"), (2, "b"))
rows = await client.fetch("SELECT * FROM t")
```

**`binary=True`** — the row-oriented `RowBinary` format. Faster than TSV in both
directions, and the right choice when streaming row-by-row with `iterate`.

```python
client = ChClient(session, binary=True)
```

Because binary encoding is type-specific, a binary INSERT first looks up the
target column types (one lightweight query) and encodes the rows to match them.

A couple of `native=True` caveats: `decode=False` (raw bytes) is a TSV-only
feature, and the Native format does not carry a column's timezone, so a tz-aware
`DateTime`/`DateTime64` comes back as a naive UTC `datetime` (TSV and RowBinary
apply the timezone).

#### Speed

Engine comparison on mixed-type rows, fully decoded, with the Cython extension
built:

| Engine      | SELECT (decode) | INSERT         |
|:------------|----------------:|---------------:|
| `TSV`       | ~305k rows/sec  | ~245k rows/sec |
| `RowBinary` | ~370k rows/sec  | ~320k rows/sec |
| `Native`    | ~730k rows/sec  | ~330k rows/sec |

Against the popular Python ClickHouse clients on the same workload, the Native
engine is the fastest async SELECT option short of clickhouse-connect, and ahead
of the synchronous clickhouse-driver:

| Client                              | SELECT        | INSERT        |
|:------------------------------------|--------------:|--------------:|
| aiochclient — Native (HTTP, async)  | ~730k rows/sec | ~330k rows/sec |
| clickhouse-connect (HTTP, async)    | ~820k rows/sec | ~450k rows/sec |
| clickhouse-driver (native, sync)    | ~440k rows/sec | ~370k rows/sec |
| asynch (native, async)              | ~108k rows/sec | ~165k rows/sec |

Indicative best-of-8 figures (Apple M1 Pro, ClickHouse 26.5, single connection);
they vary run to run and with the data. Full methodology lives in
[BENCHMARK_RESULTS.md](BENCHMARK_RESULTS.md); reproduce with `benchmarks_vs_libs.py`
(clients) and `benchmarks.py` (engines).

## Documentation

To check out the [api docs](https://aiochclient.readthedocs.io/en/latest/api.html),
visit the [readthedocs site.](https://aiochclient.readthedocs.io/en/latest/).

## Type Conversion

`aiochclient` automatically converts types from ClickHouse to python types and
vice-versa.


| ClickHouse type      | Python type             |
|:---------------------|:------------------------|
| `Bool`               | `bool`                  |
| `UInt8`              | `int`                   |
| `UInt16`             | `int`                   |
| `UInt32`             | `int`                   |
| `UInt64`             | `int`                   |
| `UInt128`            | `int`                   |
| `UInt256`            | `int`                   |
| `Int8`               | `int`                   |
| `Int16`              | `int`                   |
| `Int32`              | `int`                   |
| `Int64`              | `int`                   |
| `Int128`             | `int`                   |
| `Int256`             | `int`                   |
| `Float32`            | `float`                 |
| `Float64`            | `float`                 |
| `String`             | `str`                   |
| `FixedString`        | `str`                   |
| `Enum8`              | `str`                   |
| `Enum16`             | `str`                   |
| `Date`               | `datetime.date`         |
| `DateTime`           | `datetime.datetime`     |
| `DateTime64`         | `datetime.datetime`     |
| `Decimal`            | `decimal.Decimal`       |
| `Decimal32`          | `decimal.Decimal`       |
| `Decimal64`          | `decimal.Decimal`       |
| `Decimal128`         | `decimal.Decimal`       |
| `IPv4`               | `ipaddress.IPv4Address` |
| `IPv6`               | `ipaddress.IPv6Address` |
| `UUID`               | `uuid.UUID`             |
| `Nothing`            | `None`                  |
| `Tuple(T1, T2, ...)` | `Tuple[T1, T2, ...]`    |
| `Array(T)`           | `List[T]`               |
| `Nullable(T)`        | `None` or `T`           |
| `LowCardinality(T)`  | `T`                     |
| `Map(T1, T2)`        | `Dict[T1, T2]`          |
| `Nested(T1, T2, ...)` | `List[Tuple[T1, T2, ...], Tuple[T1, T2, ...]]` |


## Connection Pool Settings

`aiochclient` uses the
[aiohttp.TCPConnector](https://docs.aiohttp.org/en/stable/client_advanced.html#limiting-connection-pool-size)
to determine pool size. By default, the pool limit is 100 open connections.

## Notes on Speed

It's highly recommended using `uvloop` and installing `aiochclient` with
speedups for the sake of speed. Some recent benchmarks on our
machines without parallelization:

- 180k-220k rows/sec on SELECT
- 50k-80k rows/sec on INSERT

_Note: these benchmarks are system dependent_
