import datetime as dt

import pytest
import aiohttp
from aiochclient import ChClient, ChClientError

pytestmark = pytest.mark.asyncio


@pytest.fixture(
    params=[
        {
            "compress_response": True,
            "user": "default",
            "password": "",
            "database": "default",
        },
        {},
    ]
)
async def chclient(request):
    async with aiohttp.ClientSession() as s:
        yield ChClient(s, **request.param)


@pytest.fixture
async def is_alive(chclient):
    return await chclient.is_alive()


@pytest.fixture
async def bad_query(chclient):
    with pytest.raises(ChClientError) as exception:
        await chclient.execute("SELE")
    return exception


@pytest.fixture
async def bad_select(chclient, all_types_db):
    with pytest.raises(ChClientError) as exception:
        await chclient.execute("SELECT * FROM all_types WHERE",
                               1, 2, 3, 4)
    return exception


@pytest.fixture
async def all_types_db(chclient):
    await chclient.execute("DROP TABLE IF EXISTS all_types")
    await chclient.execute(
        """
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
                            m Nullable(Date),
                            n Nullable(DateTime),
                            o Enum8('hello' = 1, 'world' = 2),
                            p Enum16('hello' = 1000, 'world' = 2000),
                            q Array(UInt8),
                            r Tuple(UInt8, String),
                            s Nullable(Int8),
                            t Array(String),
                            esc String
                            ) ENGINE = Memory
    """
    )
    await chclient.execute(
        "INSERT INTO all_types VALUES",
        (
            1,
            1000,
            10000,
            12345678910,
            -4,
            -453,
            21322,
            -32123,
            23.432,
            -56754.564542,
            "hello man",
            "hello fixed man",
            dt.date(2018, 9, 21),
            dt.datetime(2018, 9, 21, 10, 32, 23),
            "hello",
            "world",
            [1, 2, 3, 4],
            (4, "hello"),
            None,
            [],
            "'\b\f\r\n\t\\",
        ),
        (
            2,
            1000,
            10000,
            12345678910,
            -4,
            -453,
            21322,
            -32123,
            23.432,
            -56754.564542,
            "hello man",
            "hello fixed man",
            None,
            None,
            "hello",
            "world",
            [1, 2, 3, 4],
            (4, "hello"),
            None,
            [],
            "'\b\f\r\n\t\\",
        ),
    )


@pytest.fixture(
    params=[
        ("a", 1),
        ("b", 1000),
        ("c", 10000),
        ("d", 12345678910),
        ("e", -4),
        ("f", -453),
        ("g", 21322),
        ("h", -32123),
        ("i", 23.432),
        ("j", -56754.564542),
        ("k", "hello man"),
        ("l", "hello fixed man".ljust(32, " ")),
        ("o", "hello"),
        ("p", "world"),
        ("m", dt.date(2018, 9, 21)),
        ("n", dt.datetime(2018, 9, 21, 10, 32, 23)),
        ("q", [1, 2, 3, 4]),
        ("r", (4, "hello")),
        ("s", None),
        ("t", []),
        ("esc", "'\b\f\r\n\t\\"),
    ]
)
async def select_one(chclient, all_types_db, request):
    return (
        await chclient.fetchval(f"SELECT {request.param[0]} FROM all_types WHERE a=1"),
        request.param[1],
    )


@pytest.fixture
async def select_row(chclient, all_types_db):
    return await chclient.fetchone("SELECT * FROM all_types WHERE a=1")


@pytest.fixture
async def select_all(chclient, all_types_db):
    return await chclient.fetch("SELECT * FROM all_types")


@pytest.fixture
async def select_all_stream(chclient, all_types_db):
    return [row async for row in chclient.cursor("SELECT * FROM all_types")]


async def test_is_alive(is_alive):
    assert is_alive is True


async def test_bad_query(bad_query):
    assert isinstance(bad_query, ChClientError)


async def test_bad_select(bad_select):
    assert isinstance(bad_query, ChClientError)
    assert bad_select.value == "It is possible to pass arguments only for INSERT queries"


async def test_ones(select_one):
    assert select_one[0] == select_one[1]


async def test_row(select_row):
    assert select_row == (
        1,
        1000,
        10000,
        12345678910,
        -4,
        -453,
        21322,
        -32123,
        23.432,
        -56754.564542,
        "hello man",
        "hello fixed man".ljust(32, " "),
        dt.date(2018, 9, 21),
        dt.datetime(2018, 9, 21, 10, 32, 23),
        "hello",
        "world",
        [1, 2, 3, 4],
        (4, "hello"),
        None,
        [],
        "'\b\f\r\n\t\\",
    )


async def test_rows(select_all):
    assert select_all == [
        (
            1,
            1000,
            10000,
            12345678910,
            -4,
            -453,
            21322,
            -32123,
            23.432,
            -56754.564542,
            "hello man",
            "hello fixed man".ljust(32, " "),
            dt.date(2018, 9, 21),
            dt.datetime(2018, 9, 21, 10, 32, 23),
            "hello",
            "world",
            [1, 2, 3, 4],
            (4, "hello"),
            None,
            [],
            "'\b\f\r\n\t\\",
        ),
        (
            2,
            1000,
            10000,
            12345678910,
            -4,
            -453,
            21322,
            -32123,
            23.432,
            -56754.564542,
            "hello man",
            "hello fixed man".ljust(32, " "),
            None,
            None,
            "hello",
            "world",
            [1, 2, 3, 4],
            (4, "hello"),
            None,
            [],
            "'\b\f\r\n\t\\",
        ),
    ]


async def test_stream(select_all_stream):
    assert select_all_stream == [
        (
            1,
            1000,
            10000,
            12345678910,
            -4,
            -453,
            21322,
            -32123,
            23.432,
            -56754.564542,
            "hello man",
            "hello fixed man".ljust(32, " "),
            dt.date(2018, 9, 21),
            dt.datetime(2018, 9, 21, 10, 32, 23),
            "hello",
            "world",
            [1, 2, 3, 4],
            (4, "hello"),
            None,
            [],
            "'\b\f\r\n\t\\",
        ),
        (
            2,
            1000,
            10000,
            12345678910,
            -4,
            -453,
            21322,
            -32123,
            23.432,
            -56754.564542,
            "hello man",
            "hello fixed man".ljust(32, " "),
            None,
            None,
            "hello",
            "world",
            [1, 2, 3, 4],
            (4, "hello"),
            None,
            [],
            "'\b\f\r\n\t\\",
        ),
    ]
