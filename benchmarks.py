"""
This module is used for testing speed of aiochclient after
any changes in its code. It is useful for comparing its
results with older versions speed. Results mostly depends
on the part which makes serialize and deserialize part of work
"""
import asyncio
import datetime as dt
import time
import uuid

from aiohttp import ClientSession

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
    print(
        f"- Avg time for selecting {rows} rows from {retries} runs: {avg_time} sec. Total: {total_time}"
    )


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
    print(
        f"- Avg time for selecting {rows} rows from {retries} runs: {avg_time} sec (with decoding). Total: {total_time}"
    )


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
    print(
        f"- Avg time for inserting {rows} rows from {retries} runs: {avg_time} sec. Total: {total_time}"
    )


async def main():
    await bench_selects(retries=1000, rows=1000)
    await bench_selects_with_decoding(retries=1000, rows=1000)
    await bench_inserts(retries=1000, rows=1000)


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    loop.close()
"""
Python 3.7.1 (v3.7.1:260ec2c36a, Oct 20 2018, 03:13:28)
Pure Python:
- Avg time for selecting 1000 rows from 1000 runs: 0.09247343492507934 sec
- Avg time for inserting 1000 rows from 1000 runs: 0.032303282737731934 sec

With Cython ext:
- Avg time for selecting 1000 rows from 1000 runs: 0.061105722188949586 sec
- Avg time for inserting 1000 rows from 1000 runs: 0.01820656132698059 sec

With lazy decoding:
Pure Python:
- Avg time for selecting 1000 rows from 1000 runs: 0.00917806601524353 sec.
- Avg time for selecting 1000 rows from 1000 runs: 0.08189528131484985 sec (with decoding).
- Avg time for inserting 1000 rows from 1000 runs: 0.027863742113113404 sec.

With Cython ext:
- Avg time for selecting 1000 rows from 1000 runs: 0.009102407217025757 sec.
- Avg time for selecting 1000 rows from 1000 runs: 0.05832741093635559 sec (with decoding).
- Avg time for inserting 1000 rows from 1000 runs: 0.01853747296333313 sec.
"""
