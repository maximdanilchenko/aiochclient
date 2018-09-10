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
Query example:
```python
await client.execute("CREATE TABLE t (a UInt8) ENGINE = Memory")
await client.execute("INSERT INTO t VALUES (1),(2),(3)")
```
Rows fetching:
```python
rows = await client.fetch("SELECT * FROM t")
```
Async iteration on query results steam:
```python
async for row in client.cursor(
    "SELECT number, number*2 FROM system.numbers LIMIT 10000"
):
    assert row[0] * 2 == row[1]
```
`AioChClient` returns rows as `tuple`s

## Connection pool size

If you use `aiochclient` in web apps, you can limit connection pool size with [aiohttp.TCPConnector](https://docs.aiohttp.org/en/stable/client_advanced.html#limiting-connection-pool-size).
