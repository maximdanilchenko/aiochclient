"""Compare aiochclient against the popular Python ClickHouse clients.

Real-world-ish workload: a wide-ish, mixed-type table; fully materialize every
row on SELECT and bulk-insert rows on INSERT. Throughput is reported as
rows/sec (best of N sequential runs, single connection, no async fan-out).

Run:  python benchmarks_vs_libs.py
Needs a ClickHouse server with HTTP (8123) and native (9000) ports.
"""

import asyncio
import time
from datetime import date, datetime

from aiohttp import ClientSession

from aiochclient import ChClient

HTTP_PORT = 8123
NATIVE_PORT = 9000
ROWS = 50_000
RETRIES = 8

DDL = """
CREATE TABLE bench_libs (
    id UInt32,
    value Int64,
    score Float64,
    name String,
    created Date,
    ts DateTime,
    tags Array(String),
    opt Nullable(Int32)
) ENGINE = Memory
"""
COLUMNS = ["id", "value", "score", "name", "created", "ts", "tags", "opt"]
_DATE = date(2021, 6, 1)
_TS = datetime(2021, 6, 1, 12, 30, 0)


def make_rows(n):
    return [
        (
            i,
            i * 1000,
            3.14159,
            f"name_{i}",
            _DATE,
            _TS,
            ["alpha", "beta", "gamma"],
            (i if i % 2 else None),
        )
        for i in range(n)
    ]


ROWS_DATA = make_rows(ROWS)


def speed(seconds):
    return int(ROWS / seconds)


async def best(coro_factory):
    await coro_factory()  # warmup
    best_time = float("inf")
    for _ in range(RETRIES):
        start = time.perf_counter()
        await coro_factory()
        best_time = min(best_time, time.perf_counter() - start)
    return speed(best_time)


def best_sync(fn):
    fn()  # warmup
    best_time = float("inf")
    for _ in range(RETRIES):
        start = time.perf_counter()
        fn()
        best_time = min(best_time, time.perf_counter() - start)
    return speed(best_time)


# ---------------------------------------------------------------- aiochclient
async def bench_aiochclient(engine):
    async with ClientSession() as session:
        prep = ChClient(session)
        await prep.execute("DROP TABLE IF EXISTS bench_libs")
        await prep.execute(DDL)
        client = ChClient(
            session,
            binary=(engine == "rowbinary"),
            native=(engine == "native"),
        )

        # INSERT
        async def do_insert():
            await prep.execute("TRUNCATE TABLE bench_libs")
            await client.execute("INSERT INTO bench_libs VALUES", *ROWS_DATA)

        ins = await best(do_insert)

        # SELECT (materialize every row)
        await prep.execute("TRUNCATE TABLE bench_libs")
        await client.execute("INSERT INTO bench_libs VALUES", *ROWS_DATA)

        async def do_select():
            rows = await client.fetch("SELECT * FROM bench_libs")
            return [row[:] for row in rows]

        sel = await best(do_select)
        return sel, ins


# ----------------------------------------------------------- clickhouse-connect
async def bench_clickhouse_connect():
    import clickhouse_connect

    client = await clickhouse_connect.get_async_client(host="localhost", port=HTTP_PORT)
    await client.command("DROP TABLE IF EXISTS bench_libs")
    await client.command(DDL)

    async def do_insert():
        await client.command("TRUNCATE TABLE bench_libs")
        await client.insert("bench_libs", ROWS_DATA, column_names=COLUMNS)

    ins = await best(do_insert)

    await client.command("TRUNCATE TABLE bench_libs")
    await client.insert("bench_libs", ROWS_DATA, column_names=COLUMNS)

    async def do_select():
        return (await client.query("SELECT * FROM bench_libs")).result_rows

    sel = await best(do_select)
    await client.close()
    return sel, ins


# ----------------------------------------------------------------------- asynch
async def bench_asynch():
    import asynch

    conn = asynch.Connection(host="localhost", port=NATIVE_PORT)
    await conn.connect()
    async with conn.cursor() as cur:
        await cur.execute("DROP TABLE IF EXISTS bench_libs")
        await cur.execute(DDL)

        async def do_insert():
            await cur.execute("TRUNCATE TABLE bench_libs")
            await cur.execute("INSERT INTO bench_libs VALUES", ROWS_DATA)

        ins = await best(do_insert)

        await cur.execute("TRUNCATE TABLE bench_libs")
        await cur.execute("INSERT INTO bench_libs VALUES", ROWS_DATA)

        async def do_select():
            await cur.execute("SELECT * FROM bench_libs")
            return await cur.fetchall()

        sel = await best(do_select)
    await conn.close()
    return sel, ins


# ------------------------------------------------------------- clickhouse-driver
def bench_clickhouse_driver():
    from clickhouse_driver import Client

    client = Client(host="localhost", port=NATIVE_PORT)
    client.execute("DROP TABLE IF EXISTS bench_libs")
    client.execute(DDL)

    def do_insert():
        client.execute("TRUNCATE TABLE bench_libs")
        client.execute("INSERT INTO bench_libs VALUES", ROWS_DATA)

    ins = best_sync(do_insert)

    client.execute("TRUNCATE TABLE bench_libs")
    client.execute("INSERT INTO bench_libs VALUES", ROWS_DATA)

    def do_select():
        return client.execute("SELECT * FROM bench_libs")

    sel = best_sync(do_select)
    client.disconnect()
    return sel, ins


async def main():
    results = []

    async def run(label, factory):
        try:
            sel, ins = await factory()
            results.append((label, sel, ins))
            print(f"  {label:<32} select {sel:>8} rows/sec | insert {ins:>8} rows/sec")
        except Exception as exc:  # noqa: BLE001
            results.append((label, None, None))
            print(f"  {label:<32} ERROR: {type(exc).__name__}: {exc}")

    print(f"Benchmark: {ROWS} rows, best of {RETRIES} runs\n")
    await run("aiochclient (HTTP, TSV)", lambda: bench_aiochclient("tsv"))
    await run("aiochclient (HTTP, RowBinary)", lambda: bench_aiochclient("rowbinary"))
    await run("aiochclient (HTTP, Native)", lambda: bench_aiochclient("native"))
    await run("clickhouse-connect (HTTP)", bench_clickhouse_connect)
    await run("asynch (native, async)", bench_asynch)
    await run(
        "clickhouse-driver (native, sync)",
        lambda: asyncio.get_event_loop().run_in_executor(None, bench_clickhouse_driver),
    )
    return results


if __name__ == "__main__":
    asyncio.run(main())
