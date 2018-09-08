# aiochclient
### Async http clickhouse client for python 3.6+ with types converting and streaming support


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
    "SELECT number, number*2 as double FROM system.numbers LIMIT 10000"
):
    assert row.number * 2 == row.double
```
`ChClient` returns rows as `namedtuple`s
