import asyncio
import logging
from aiohttp import ClientSession
from aiochclient import AioChClient


async def main():
    async with ClientSession() as s:
        client = AioChClient(s, compress_response=True)
        assert await client.is_alive()
        await client.execute("DROP TABLE IF EXISTS all_types")
        await client.execute("""
        CREATE TABLE all_types (a UInt8, 
                                b UInt16,
                                c UInt32,
                                d UInt64,
                                e Int8,
                                f Int16,
                                g Int32,
                                h Int64,
                                i Float32,
                                j Float64,
                                k String,
                                l FixedString(32),
                                m Date,
                                n DateTime,
                                o Enum8('hello' = 1, 'world' = 2),
                                p Enum16('hello' = 1000, 'world' = 2000),
                                q Array(UInt8),
                                r Tuple(UInt8, String),
                                s Nullable(Int8)
                                ) ENGINE = Memory
        """)
        await client.execute("""
        INSERT INTO all_types values (1, 1000, 10000, 12345678910, -4, -453, 21322, -32123, 23.432, -56754.564542,
                                      'hello man', 'hello fixed man', '2018-09-21', '2018-09-21 10:32:23',
                                      'hello', 'world', [1,2,3,4], (4, 'hello'), NULL),
                                      (0, 999, 9999, 99945678913, 4, 53, -221322, 453123, 23.432, 12324.5632,
                                      '\b\f\r\n\t', 'hello fixed man', '2018-09-22', '2018-09-22 12:02:23',
                                      'hello', 'world', [1,2,3,4], (65, 'world'), 4)
        """)
        async for record in client.cursor("SELECT * FROM all_types"):
            print(record)


if __name__ == "__main__":
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    logger.addHandler(logging.StreamHandler())

    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    loop.close()
    # TSVWithNamesAndTypes
