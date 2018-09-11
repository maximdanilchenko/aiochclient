# aiochclient
### Async http(s) clickhouse client for python 3.6+ with types converting and streaming support

![Status: developing](https://img.shields.io/badge/status-developing-red.svg) 


## Quick start

`aiochclient` needs `aiohttp.ClientSession` for connecting:

```python
from aiochclient import AioChClient
from aiohttp import ClientSession


async def main():
    async with ClientSession() as s:
        client = AioChClient(s)
        assert await client.is_alive()  # returns True if connection is Ok

```
### Query example:
```python
await client.execute("CREATE TABLE t (a UInt8, b Tuple(Date, Nullable(Float32))) ENGINE = Memory")
await client.execute("INSERT INTO t VALUES (1, ('2018-09-07', NULL)),(2, ('2018-09-08', 3.14))")
```
### Rows fetching:
For fetching all rows at once use `fetch` method:
```python
all_rows = await client.fetch("SELECT * FROM t")
```
For fetching first row from result use `fetchone` method:
```python
assert (await client.fetchone("SELECT * FROM t WHERE a=1")) == (1, (dt.date(2018, 9, 7), None))
```
You can also use `fetchval` method, which returns 
first value of the first row from query result:
```python
assert await client.fetchval("EXISTS TABLE t")
```
Async iteration on query results steam:
```python
async for row in client.cursor(
    "SELECT number, number*2 FROM system.numbers LIMIT 10000"
):
    assert row[0] * 2 == row[1]
```
`AioChClient` returns rows as `tuple`s.

## Types converting

`aiochclient` automatically converts values to needed type from Clickhouse response.

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
| `Date` | `dt.date` |
| `DateTime` | `da.datetime` |
| `Tuple(T1, T2, ...)` | `tuple(T1, T2, ...)` |
| `Array(T)` | `list(T)` |
| `Nullable(T)` | `None` or `T` |
| `Nothing` | `None` |

## Connection pool size

If you use `aiochclient` in web apps, you can limit connection pool size with 
[aiohttp.TCPConnector](https://docs.aiohttp.org/en/stable/client_advanced.html#limiting-connection-pool-size).
