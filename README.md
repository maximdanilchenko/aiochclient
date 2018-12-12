# aiochclient
### Async http(s) clickhouse client for python 3.6+ with types converting in both directions and streaming support

[![PyPI version](https://badge.fury.io/py/aiochclient.svg)](https://badge.fury.io/py/aiochclient)
[![Travis CI](https://travis-ci.org/maximdanilchenko/aiochclient.svg?branch=master)](https://travis-ci.org/maximdanilchenko/aiochclient)
[![Documentation Status](https://readthedocs.org/projects/aiochclient/badge/?version=latest)](https://aiochclient.readthedocs.io/en/latest/?badge=latest)
[![codecov](https://codecov.io/gh/maximdanilchenko/aiochclient/branch/master/graph/badge.svg)](https://codecov.io/gh/maximdanilchenko/aiochclient)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/ambv/black)

## Install
```
> pip install aiochclient
```

When installing aiochclient will try to build C extensions to boost its speed (about 30% speed up).

## Quick start

`aiochclient` needs `aiohttp.ClientSession` for connecting:

```python
from aiochclient import ChClient
from aiohttp import ClientSession


async def main():
    async with ClientSession() as s:
        client = ChClient(s)
        assert await client.is_alive()  # returns True if connection is Ok

```
## Query examples
```python
await client.execute(
    "CREATE TABLE t (a UInt8, b Tuple(Date, Nullable(Float32))) ENGINE = Memory"
)
```
For INSERT queries you can pass values as `*args`. Values should be iterables.
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
For fetching first row from result use `fetchone` method:
```python
row = await client.fetchone("SELECT * FROM t WHERE a=1")
assert row == (1, (dt.date(2018, 9, 7), None))
```
You can also use `fetchval` method, which returns 
first value of the first row from query result:
```python
val = await client.fetchval("SELECT b FROM t WHERE a=2")
assert val == (dt.date(2018, 9, 8), 3.14)
```
Async iteration on query results steam:
```python
async for row in client.cursor(
    "SELECT number, number*2 FROM system.numbers LIMIT 10000"
):
    assert row[0] * 2 == row[1]
```

`ChClient` returns rows as `tuple`s.

Use `fetch`/`fetchrow`/`fetchone` for SELECT queries 
and `execute` or any of last for INSERT and all another queries.

## Types converting

`aiochclient` automatically converts values to needed type both 
from Clickhouse response and for client INSERT queries.

| Clickhouse type | Python type |
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
| `Tuple(T1, T2, ...)` | `tuple(T1, T2, ...)` |
| `Array(T)` | `list(T)` |
| `UUID` | `uuid.UUID` |
| `Nullable(T)` | `None` or `T` |
| `Nothing` | `None` |

## Connection pool

If you use `aiochclient` in web apps, you can limit connection pool size with 
[aiohttp.TCPConnector](https://docs.aiohttp.org/en/stable/client_advanced.html#limiting-connection-pool-size).
