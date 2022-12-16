import asyncio

from aiohttp import ClientSession

from aiochclient import AsyncClient


async def main():
    async with ClientSession() as s:
        client = AsyncClient(s, url="http://localhost:8123")
        assert await client.is_alive()


if __name__ == "__main__":
    # if >=3.7:
    asyncio.run(main())

    # if <3.7:
    # loop = asyncio.get_event_loop()
    # loop.run_until_complete(main())
    # loop.close()
