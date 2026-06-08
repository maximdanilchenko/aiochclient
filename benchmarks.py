"""
This module is used for testing speed of aiochclient after
any changes in its code. It is useful for comparing its
results with older versions speed. Results mostly depends
on the part which makes serialize and deserialize work.

Pay attention to that fact, that this benchmark is just one
task (all queries go one by one) without async staff.
So you can get much more speed from parallelling all those
IO with asyncio instruments.

=== Last Results ============================================
== Python 3.7.1 (v3.7.1:260ec2c36a, Oct 20 2018, 03:13:28) ==
= Pure Python ===============================================
AIOCHCLIENT
= Pure Python ==============================================
selects
- Avg time for selecting 10000 rows from 100 runs: 0.0614057207107544 sec. Total: 6.1405720710754395
  Speed: 162851 rows/sec
selects with decoding
- Avg time for selecting 10000 rows from 100 runs: 0.2676021885871887 sec (with decoding). Total: 26.760218858718872
  Speed: 37368 rows/sec
inserts
- Avg time for inserting 10000 rows from 100 runs: 0.2058545708656311 sec. Total: 20.58545708656311
  Speed: 48577 rows/sec
= With Cython ext ===========================================
selects
- Avg time for selecting 10000 rows from 100 runs: 0.0589641809463501 sec. Total: 5.89641809463501
  Speed: 169594 rows/sec
selects with decoding
- Avg time for selecting 10000 rows from 100 runs: 0.14500005006790162 sec (with decoding). Total: 14.500005006790161
  Speed: 68965 rows/sec
inserts
- Avg time for inserting 10000 rows from 100 runs: 0.13569785118103028 sec. Total: 13.569785118103027
  Speed: 73693 rows/sec
AIOCH
selects with decoding
- Avg time for selecting 10000 rows from 100 runs: 0.32889273166656496 sec. Total: 32.889273166656494
  Speed: 30405 rows/sec
"""
import asyncio
import datetime as dt
import time
import uuid

from aiohttp import ClientSession

from aiochclient import ChClient

try:
    import uvloop
except ImportError:
    uvloop = None

try:
    from aioch import Client
except ImportError:
    Client = None


def row_data():
    return (
        1,
        2,
        3.14,
        "hello",
        "world world \nman",
        dt.date.today(),
        dt.datetime.utcnow(),
        "hello",
        None,
        ["q", "w", "e", "r"],
        uuid.uuid4(),
    )


async def prepare_db(client):
    await client.execute("DROP TABLE IF EXISTS benchmark_tbl")
    await client.execute(
        """
        CREATE TABLE benchmark_tbl ( 
                            a UInt16,
                            b Int16,
                            c Float32,
                            d String,
                            e FixedString(16),
                            f Date,
                            g DateTime,
                            h Enum16('hello' = 1, 'world' = 2),
                            j Nullable(Int8),
                            k Array(String),
                            u UUID
        ) ENGINE = Memory
        """
    )


async def insert_rows(client, test_data, number):
    await client.execute(
        "INSERT INTO benchmark_tbl VALUES", *(test_data for _ in range(number))
    )


async def bench_selects(*, retries: int, rows: int):
    print("AIOCHCLIENT selects")
    async with ClientSession() as s:
        client = ChClient(s)
        # prepare environment
        await prepare_db(client)
        await insert_rows(client, row_data(), rows)
        # actual testing
        start_time = time.time()
        for _ in range(retries):
            await client.fetch("SELECT * FROM benchmark_tbl")
        total_time = time.time() - start_time
        avg_time = total_time / retries
        speed = int(1 / avg_time * rows)
    print(
        f"- Avg time for selecting {rows} rows from {retries} runs: {avg_time} sec. Total: {total_time}"
    )
    print(f"  Speed: {speed} rows/sec")


async def bench_selects_with_decoding(*, retries: int, rows: int):
    print("AIOCHCLIENT selects with decoding")
    async with ClientSession() as s:
        client = ChClient(s, compress_response=True)
        # prepare environment
        await prepare_db(client)
        await insert_rows(client, row_data(), rows)
        # actual testing
        start_time = time.time()
        for _ in range(retries):
            selected_rows = await client.fetch("SELECT * FROM benchmark_tbl")
            # decoding:
            selected_rows = [row[0] for row in selected_rows]
        total_time = time.time() - start_time
        avg_time = total_time / retries
        speed = int(1 / avg_time * rows)
    print(
        f"- Avg time for selecting {rows} rows from {retries} runs: {avg_time} sec (with decoding). Total: {total_time}"
    )
    print(f"  Speed: {speed} rows/sec")


async def bench_inserts(*, retries: int, rows: int):
    print("AIOCHCLIENT inserts")
    async with ClientSession() as s:
        client = ChClient(s, compress_response=True)
        # prepare environment
        await prepare_db(client)
        # actual testing
        one_row = row_data()
        start_time = time.time()
        for _ in range(retries):
            await client.execute(
                "INSERT INTO benchmark_tbl VALUES", *(one_row for _ in range(rows))
            )
        total_time = time.time() - start_time
        avg_time = total_time / retries
        speed = int(1 / avg_time * rows)
    print(
        f"- Avg time for inserting {rows} rows from {retries} runs: {avg_time} sec. Total: {total_time}"
    )
    print(f"  Speed: {speed} rows/sec")


async def bench_selects_aioch_with_decoding(*, retries: int, rows: int):
    print("AIOCH selects with decoding")
    client = Client(host='localhost')
    # prepare environment
    await prepare_db(client)
    await client.execute(
        "INSERT INTO benchmark_tbl VALUES", list(row_data() for _ in range(rows))
    )
    # actual testing
    start_time = time.time()
    for _ in range(retries):
        selected_rows = await client.execute("SELECT * FROM benchmark_tbl")
        selected_rows = [row[0] for row in selected_rows]
    total_time = time.time() - start_time
    avg_time = total_time / retries
    speed = int(1 / avg_time * rows)
    print(
        f"- Avg time for selecting {rows} rows from {retries} runs: {avg_time} sec. Total: {total_time}"
    )
    print(f"  Speed: {speed} rows/sec")


async def bench_formats(*, retries: int, rows: int):
    """Compare the TSV and RowBinary engines for select-decode and insert."""
    print("TSV vs RowBinary")
    one_row = row_data()
    async with ClientSession() as session:
        prep = ChClient(session)
        for label, binary in (("TSV      ", False), ("RowBinary", True)):
            client = ChClient(session, binary=binary)
            # SELECT (with full decoding) over a fixed `rows`-row table
            await prepare_db(prep)
            await insert_rows(prep, one_row, rows)
            await client.fetch("SELECT * FROM benchmark_tbl")  # warmup
            start = time.time()
            for _ in range(retries):
                selected = await client.fetch("SELECT * FROM benchmark_tbl")
                _ = [row[:] for row in selected]
            sel_speed = int(rows / ((time.time() - start) / retries))
            # INSERT — clear before each run (not timed) to avoid accumulation
            insert_total = 0.0
            for _ in range(retries):
                await prepare_db(prep)
                start = time.time()
                await client.execute(
                    "INSERT INTO benchmark_tbl VALUES",
                    *(one_row for _ in range(rows)),
                )
                insert_total += time.time() - start
            ins_speed = int(rows / (insert_total / retries))
            print(
                f"  {label}  select-decode: {sel_speed:>8} rows/sec"
                f" | insert: {ins_speed:>8} rows/sec"
            )


async def main():
    await bench_selects(retries=100, rows=10000)
    await bench_selects_with_decoding(retries=100, rows=10000)
    await bench_inserts(retries=100, rows=10000)
    await bench_formats(retries=50, rows=10000)

    if Client is not None:
        await bench_selects_aioch_with_decoding(retries=100, rows=10000)


if __name__ == "__main__":
    if uvloop is not None:
        uvloop.install()
    asyncio.run(main())
