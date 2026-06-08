"""Focused Native-SELECT micro-benchmark + breakdown for the 1M rows/sec goal.

Mirrors the SELECT workload of benchmarks_vs_libs.py (50k mixed-type rows, fully
materialized) but isolates the Native engine and reports a breakdown so each
optimization iteration can see *where* the time goes.

Run:  python perf_iterations/bench_native.py [--profile]
"""

import asyncio
import sys
import time
from datetime import date, datetime

from aiohttp import ClientSession

from aiochclient import ChClient
from aiochclient.native import blocks_from_native, decode_column_with_prefix

ROWS = 50_000
RETRIES = 12

DDL = """
CREATE TABLE bench_native (
    id UInt32, value Int64, score Float64, name String, created Date,
    ts DateTime, tags Array(String), opt Nullable(Int32)
) ENGINE = Memory
"""
_DATE = date(2021, 6, 1)
_TS = datetime(2021, 6, 1, 12, 30, 0)
ROWS_DATA = [
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
    for i in range(ROWS)
]


def speed(seconds):
    return int(ROWS / seconds)


async def best(fn):
    await fn()
    b = 1e9
    for _ in range(RETRIES):
        t = time.perf_counter()
        await fn()
        b = min(b, time.perf_counter() - t)
    return b


def _native_source(client, query):
    return client._http_client.post_return_bytes(
        url=client.url,
        params={**client.params},
        headers=client.headers,
        data=(query + " FORMAT Native").encode(),
    )


async def main():
    async with ClientSession() as session:
        prep = ChClient(session)
        await prep.execute("DROP TABLE IF EXISTS bench_native")
        await prep.execute(DDL)
        await prep.execute("INSERT INTO bench_native VALUES", *ROWS_DATA)
        client = ChClient(session, native=True)

        q = "SELECT * FROM bench_native"

        async def headline():
            rows = await client.fetch(q)
            return [row[:] for row in rows]

        async def fetch_only():
            return await client.fetch(q)

        async def blocks_count():
            n = 0
            async for blk in blocks_from_native(_native_source(client, q)):
                n += len(blk)
            return n

        async def decode_only():
            # decode every column to lists, no Record, no transpose
            from aiochclient.binary import BinaryReader, read_binary_str

            reader = BinaryReader(_native_source(client, q))
            total = 0
            while True:
                try:
                    nc = await reader.parse(lambda c: c.read_varint())
                except Exception:
                    break
                nr = await reader.parse(lambda c: c.read_varint())
                cols = []
                for _ in range(nc):
                    await reader.parse(read_binary_str)
                    ct = (await reader.parse(read_binary_str)).decode()
                    cols.append(
                        await reader.parse(
                            lambda c, ct=ct, nr=nr: decode_column_with_prefix(c, nr, ct)
                        )
                    )
                total += nr
            return total

        print(f"Native SELECT breakdown: {ROWS} rows, best of {RETRIES}\n")
        print(
            f"  decode columns only      : {speed(await best(decode_only)):>9} rows/sec"
        )
        print(
            f"  blocks -> Records (count) : {speed(await best(blocks_count)):>9} rows/sec"
        )
        print(
            f"  fetch (build list)        : {speed(await best(fetch_only)):>9} rows/sec"
        )
        print(
            f"  fetch + row[:] (HEADLINE) : {speed(await best(headline)):>9} rows/sec"
        )

        await prep.execute("DROP TABLE IF EXISTS bench_native")


def profile():
    import cProfile
    import pstats

    async def run():
        async with ClientSession() as session:
            prep = ChClient(session)
            await prep.execute("DROP TABLE IF EXISTS bench_native")
            await prep.execute(DDL)
            await prep.execute("INSERT INTO bench_native VALUES", *ROWS_DATA)
            client = ChClient(session, native=True)
            for _ in range(20):
                rows = await client.fetch("SELECT * FROM bench_native")
                [row[:] for row in rows]
            await prep.execute("DROP TABLE IF EXISTS bench_native")

    pr = cProfile.Profile()
    pr.enable()
    asyncio.run(run())
    pr.disable()
    stats = pstats.Stats(pr).sort_stats("tottime")
    stats.print_stats(25)


if __name__ == "__main__":
    if "--profile" in sys.argv:
        profile()
    else:
        asyncio.run(main())
