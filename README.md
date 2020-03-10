# aiochclient
### Async http(s) ClickHouse client for python 3.6+ with types converting in both directions, streaming support, lazy decoding on select queries and fully typed interface

[![PyPI version](https://badge.fury.io/py/aiochclient.svg)](https://badge.fury.io/py/aiochclient)
[![Travis CI](https://travis-ci.org/maximdanilchenko/aiochclient.svg?branch=master)](https://travis-ci.org/maximdanilchenko/aiochclient)
[![Documentation Status](https://readthedocs.org/projects/aiochclient/badge/?version=latest)](https://aiochclient.readthedocs.io/en/latest/?badge=latest)
[![codecov](https://codecov.io/gh/maximdanilchenko/aiochclient/branch/master/graph/badge.svg)](https://codecov.io/gh/maximdanilchenko/aiochclient)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/ambv/black)

## Contents

- [Install](#install)
- [Quick start](#quick-start)
- [Types converting](#types-converting)
- [Connection pool](#connection-pool)
- [Speed](#speed)

## Install
```
> pip install aiochclient
```
Or to install with extras requirements for speedup:
```
> pip install aiochclient[speedups]
```
It will additionally install [cChardet](https://pypi.python.org/pypi/cchardet) 
and [aiodns](https://pypi.python.org/pypi/aiodns) for `aiohttp` speedup 
and [ciso8601](https://github.com/closeio/ciso8601) for ultra fast 
datetime parsing while decoding data from ClickHouse.

Also while installing it will try to build Cython extensions for speed boost (about 30%).

## Quick start

### Connecting to ClickHouse

`aiochclient` needs `aiohttp.ClientSession` for connecting:

```python
from aiochclient import ChClient
from aiohttp import ClientSession


async def main():
    async with ClientSession() as s:
        client = ChClient(s)
        assert await client.is_alive()  # returns True if connection is Ok

```

### Making queries
```python
await client.execute(
    "CREATE TABLE t (a UInt8, b Tuple(Date, Nullable(Float32))) ENGINE = Memory"
)
```
For INSERT queries you can pass values as `*args`. Values should be iterables:
```python
await client.execute(
    "INSERT INTO t VALUES",
    (1, (dt.date(2018, 9, 7), None)),
    (2, (dt.date(2018, 9, 8), 3.14)),
)
```
For fetching all rows at once use `fetch` method:
```python
all_rows = await client.fetch("SELECT * FROM t")
```
For fetching first row from result use `fetchrow` method:
```python
row = await client.fetchrow("SELECT * FROM t WHERE a=1")

assert row[0] == 1
assert row["b"] == (dt.date(2018, 9, 7), None)
```
You can also use `fetchval` method, which returns 
first value of the first row from query result:
```python
val = await client.fetchval("SELECT b FROM t WHERE a=2")

assert val == (dt.date(2018, 9, 8), 3.14)
```
With async iteration on query results steam you can fetch 
multiple rows without loading them all into memory at once:
```python
async for row in client.iterate(
    "SELECT number, number*2 FROM system.numbers LIMIT 10000"
):
    assert row[0] * 2 == row[1]
```

Use `fetch`/`fetchrow`/`fetchval`/`iterate` for SELECT queries 
and `execute` or any of last for INSERT and all another queries.

### Working with query results
All fetch queries return rows as lightweight, memory 
efficient objects (**from v`1.0.0`, before it - just tuples**)
with full mapping interface, where 
you can get fields by names or by indexes: 
```python
row = await client.fetchrow("SELECT a, b FROM t WHERE a=1")

assert row["a"] == 1
assert row[0] == 1
assert row[:] == (1, (dt.date(2018, 9, 8), 3.14))
assert list(row.keys()) == ["a", "b"]
assert list(row.values()) == [1, (dt.date(2018, 9, 8), 3.14)]
```

## Types converting

`aiochclient` automatically converts values to needed type both 
from ClickHouse response and for client INSERT queries.

| ClickHouse type | Python type |
|:----------------|:------------|
| `UInt8` | `int` |
| `UInt16` | `int` |
| `UInt32` | `int` |
| `UInt64` | `int` |
| `Int8` | `int` |
| `Int16` | `int` |
| `Int32` | `int` |
| `Int64` | `int` |
| `Float32` | `float` |
| `Float64` | `float` |
| `String` | `str` |
| `FixedString` | `str` |
| `Enum8` | `str` |
| `Enum16` | `str` |
| `Date` | `datetime.date` |
| `DateTime` | `datetime.datetime` |
| `Decimal` | `decimal.Decimal` |
| `Decimal32` | `decimal.Decimal` |
| `Decimal64` | `decimal.Decimal` |
| `Decimal128` | `decimal.Decimal` |
| `IPv4` | `ipaddress.IPv4Address` |
| `IPv6` | `ipaddress.IPv6Address` |
| `UUID` | `uuid.UUID` |
| `Nothing` | `None` |
| `Tuple(T1, T2, ...)` | `Tuple[T1, T2, ...]` |
| `Array(T)` | `List[T]` |
| `Nullable(T)` | `None` or `T` |
| `LowCardinality(T)` | `T` |

## Connection pool

If you want to change connection pool size, you can use 
[aiohttp.TCPConnector](https://docs.aiohttp.org/en/stable/client_advanced.html#limiting-connection-pool-size). 
Note that by default pool limit is 100 connections.

## Speed

Using of `uvloop` and installing with `aiochclient[speedups]`
is highly recommended for sake of speed. 

As for the last version of `aiochclient` its speed 
using one task (without gather or parallel 
clients and so on) is about 
**180k-220k rows/sec** on SELECT and about 
**50k-80k rows/sec** on INSERT queries 
depending on its environment and ClickHouse settings.

------

Please star️ this repository if this project helped you!
