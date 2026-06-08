"""Focused Native-INSERT micro-benchmark + breakdown (goal: +30% throughput).

Mirrors the INSERT workload of benchmarks_vs_libs.py (50k mixed-type rows) but
isolates the Native write path and splits the cost into: pure encode
(rows_to_native, no IO), the LIMIT 0 column-types probe round-trip, and the full
client.execute INSERT.

Run:  python perf_insert/bench_insert.py [--profile]
"""

import asyncio
import sys
import time
from datetime import date, datetime

from aiohttp import ClientSession

from aiochclient import ChClient
from aiochclient.native import rows_to_native

ROWS = 50_000
RETRIES = 10

DDL = """
CREATE TABLE bench_insert (
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


async def main():
    async with ClientSession() as session:
        prep = ChClient(session)
        await prep.execute("DROP TABLE IF EXISTS bench_insert")
        await prep.execute(DDL)
        client = ChClient(session, native=True)

        names, types = await client._fetch_insert_column_header(
            "INSERT INTO bench_insert VALUES"
        )

        # 1. pure encode, no IO
        def encode_only():
            return rows_to_native(ROWS_DATA, names, types)

        def encode_best():
            encode_only()
            b = 1e9
            for _ in range(RETRIES):
                t = time.perf_counter()
                encode_only()
                b = min(b, time.perf_counter() - t)
            return b

        # 2. probe round-trip only
        async def probe_only():
            await client._fetch_insert_column_header("INSERT INTO bench_insert VALUES")

        # 3. full native INSERT (truncate each run)
        async def full_insert():
            await prep.execute("TRUNCATE TABLE bench_insert")
            await client.execute("INSERT INTO bench_insert VALUES", *ROWS_DATA)

        enc = encode_best()
        prb = await best(probe_only)
        full = await best(full_insert)
        blob = rows_to_native(ROWS_DATA, names, types)

        print(f"Native INSERT breakdown: {ROWS} rows, best of {RETRIES}\n")
        print(
            f"  encode only (rows_to_native): {speed(enc):>9} rows/sec  "
            f"({len(blob)/1e6:.1f} MB block)"
        )
        print(f"  probe round-trip            : {speed(prb):>9} rows/sec-equiv")
        print(f"  full INSERT (HEADLINE)      : {speed(full):>9} rows/sec")

        await prep.execute("DROP TABLE IF EXISTS bench_insert")


def profile():
    import cProfile
    import pstats

    async def run():
        async with ClientSession() as session:
            prep = ChClient(session)
            await prep.execute("DROP TABLE IF EXISTS bench_insert")
            await prep.execute(DDL)
            client = ChClient(session, native=True)
            names, types = await client._fetch_insert_column_header(
                "INSERT INTO bench_insert VALUES"
            )
            for _ in range(40):
                rows_to_native(ROWS_DATA, names, types)
            await prep.execute("DROP TABLE IF EXISTS bench_insert")

    pr = cProfile.Profile()
    pr.enable()
    asyncio.run(run())
    pr.disable()
    pstats.Stats(pr).sort_stats("tottime").print_stats(20)


if __name__ == "__main__":
    if "--profile" in sys.argv:
        profile()
    else:
        asyncio.run(main())
