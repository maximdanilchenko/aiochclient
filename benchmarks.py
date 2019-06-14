"""
This module is used for testing speed of aiochclient after
any changes in its code. It is useful for comparing its
results with older versions speed. Results mostly depends
on the part which makes serialize and deserialize part of work

=== Last Results ============================================
== Python 3.7.1 (v3.7.1:260ec2c36a, Oct 20 2018, 03:13:28) ==
= Pure Python ===============================================
- Avg time for selecting 10000 rows from 100 runs: 0.047670061588287356 sec. Total: 4.767006158828735
  Speed: 209775.26914832063 rows/sec
- Avg time for selecting 10000 rows from 100 runs: 0.7206034588813782 sec (with decoding). Total: 72.06034588813782
  Speed: 13877.257840981505 rows/sec
- Avg time for inserting 10000 rows from 100 runs: 0.21813016176223754 sec. Total: 21.813016176223755
  Speed: 45844.18733847558 rows/sec
= With Cython ext ===========================================
- Avg time for selecting 10000 rows from 100 runs: 0.04855584144592285 sec. Total: 4.855584144592285
  Speed: 205948.44414625384 rows/sec
- Avg time for selecting 10000 rows from 100 runs: 0.5195666408538818 sec (with decoding). Total: 51.956664085388184
  Speed: 19246.809193841815 rows/sec
- Avg time for inserting 10000 rows from 100 runs: 0.14293188810348512 sec. Total: 14.29318881034851
  Speed: 69963.39398217303 rows/sec
"""
import asyncio
import datetime as dt
import time
import uuid

from aiohttp import ClientSession
import uvloop

from aiochclient import ChClient


def row_data():
    return (
        1,
        2,
        3.14,
        "hello",
        "world",
        dt.date.today(),
        dt.datetime.utcnow(),
        "hello",
        (3, "world"),
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
                            i Tuple(UInt8, String),
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
    async with ClientSession() as s:
        client = ChClient(s, compress_response=True)
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


async def main():
    await bench_selects(retries=100, rows=10000)
    await bench_selects_with_decoding(retries=100, rows=10000)
    await bench_inserts(retries=100, rows=10000)


if __name__ == "__main__":
    uvloop.install()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    loop.close()
