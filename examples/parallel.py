import asyncio

from aiohttp import ClientSession

from aiochclient import ChClient


async def some_query(client: ChClient, offset, limit):
    await client.execute(
        "INSERT INTO t VALUES", *((i, i / 2) for i in range(offset, offset + limit))
    )


async def main():
    async with ClientSession() as s:
        client = ChClient(s, url="http://localhost:8123")
        # preparing database
        await client.execute("CREATE TABLE t (a UInt8, b Float32) ENGINE = Memory")
        # making queries in parallel
        await asyncio.gather(
            some_query(client, 1000, 1000),
            some_query(client, 2000, 1000),
            some_query(client, 3000, 1000),
            some_query(client, 4000, 1000),
            some_query(client, 5000, 1000),
            some_query(client, 6000, 1000),
        )


if __name__ == "__main__":
    # if >=3.7:
    asyncio.run(main())

    # if <3.7:
    # loop = asyncio.get_event_loop()
    # loop.run_until_complete(main())
    # loop.close()
