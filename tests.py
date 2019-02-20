import datetime as dt
from uuid import uuid4

import pytest
import aiohttp
from aiochclient import ChClient, ChClientError

pytestmark = pytest.mark.asyncio


@pytest.fixture
def uuid():
    return uuid4()


@pytest.fixture
def rows(uuid):
    return [
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
            0,
            ["hello", "world"],
            "'\b\f\r\n\t\\",
            uuid,
            [uuid, uuid, uuid],
            ["hello", "world", "hello"],
            [dt.date(2018, 9, 21), dt.date(2018, 9, 22)],
            [
                dt.datetime(2018, 9, 21, 10, 32, 23),
                dt.datetime(2018, 9, 21, 10, 32, 24),
            ],
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
            None,
            [],
            [],
            [],
            [],
        ),
    ]


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
async def all_types_db(chclient, rows):
    await chclient.execute("DROP TABLE IF EXISTS all_types")
    await chclient.execute(
        """
    CREATE TABLE all_types (uint8 UInt8, 
                            uint16 UInt16,
                            uint32 UInt32,
                            uint64 UInt64,
                            int8 Int8,
                            int16 Int16,
                            int32 Int32,
                            int64 Int64,
                            float32 Float32,
                            float64 Float64,
                            string String,
                            fixed_string FixedString(32),
                            date Nullable(Date),
                            datetime Nullable(DateTime),
                            enum8 Enum8('hello' = 1, 'world' = 2),
                            enum16 Enum16('hello' = 1000, 'world' = 2000),
                            array_uint8 Array(UInt8),
                            tuple Tuple(UInt8, String),
                            nullable Nullable(Int8),
                            array_string Array(String),
                            escape_string String,
                            uuid Nullable(UUID),
                            array_uuid Array(UUID),
                            array_enum Array(Enum8('hello' = 1, 'world' = 2)),
                            array_date Array(Date),
                            array_datetime Array(DateTime)
                            
                            ) ENGINE = Memory
    """
    )
    await chclient.execute("INSERT INTO all_types VALUES", *rows)


@pytest.fixture
def class_chclient(chclient, all_types_db, rows, request):
    request.cls.ch = chclient
    request.cls.rows = rows


@pytest.mark.usefixtures("class_chclient")
class TestClient:
    async def test_is_alive(self):
        assert await self.ch.is_alive() is True

    async def test_bad_query(self):
        with pytest.raises(ChClientError):
            await self.ch.execute("SELE")

    async def test_bad_select(self):
        with pytest.raises(ChClientError):
            await self.ch.execute("SELECT * FROM all_types WHERE", 1, 2, 3, 4)


@pytest.mark.usefixtures("class_chclient")
class TestTypes:
    async def select_field(self, field):
        return await self.ch.fetchval(f"SELECT {field} FROM all_types WHERE uint8=1")

    async def test_uint8(self):
        assert await self.select_field("uint8") == 1

    async def test_uint16(self):
        assert await self.select_field("uint16") == 1000

    async def test_uint32(self):
        assert await self.select_field("uint32") == 10000

    async def test_uint64(self):
        assert await self.select_field("uint64") == 12345678910

    async def test_int8(self):
        assert await self.select_field("int8") == -4

    async def test_int16(self):
        assert await self.select_field("int16") == -453

    async def test_int32(self):
        assert await self.select_field("int32") == 21322

    async def test_int64(self):
        assert await self.select_field("int64") == -32123

    async def test_float32(self):
        assert await self.select_field("float32") == 23.432

    async def test_float64(self):
        assert await self.select_field("float64") == -56754.564542

    async def test_string(self):
        assert await self.select_field("string") == "hello man"

    async def test_fixed_string(self):
        assert await self.select_field("fixed_string") == "hello fixed man".ljust(
            32, " "
        )

    async def test_date(self):
        assert await self.select_field("date") == dt.date(2018, 9, 21)

    async def test_datetime(self):
        assert await self.select_field("datetime") == dt.datetime(
            2018, 9, 21, 10, 32, 23
        )

    async def test_enum8(self):
        assert await self.select_field("enum8") == "hello"

    async def test_enum16(self):
        assert await self.select_field("enum16") == "world"

    async def test_array_uint8(self):
        assert await self.select_field("array_uint8") == [1, 2, 3, 4]

    async def test_tuple(self):
        assert await self.select_field("tuple") == (4, "hello")

    async def test_nullable(self):
        assert await self.select_field("nullable") == 0

    async def test_array_string(self):
        assert await self.select_field("array_string") == ["hello", "world"]

    async def test_escape_string(self):
        assert await self.select_field("escape_string") == "'\b\f\r\n\t\\"

    async def test_uuid(self, uuid):
        assert await self.select_field("uuid") == uuid

    async def test_array_uuid(self, uuid):
        assert await self.select_field("array_uuid") == [uuid, uuid, uuid]

    async def test_array_enum(self):
        assert await self.select_field("array_enum ") == ["hello", "world", "hello"]

    async def test_array_date(self):
        assert await self.select_field("array_date ") == [
            dt.date(2018, 9, 21),
            dt.date(2018, 9, 22),
        ]

    async def test_array_datetime(self):
        assert await self.select_field("array_datetime ") == [
            dt.datetime(2018, 9, 21, 10, 32, 23),
            dt.datetime(2018, 9, 21, 10, 32, 24),
        ]


@pytest.mark.usefixtures("class_chclient")
class TestFetching:
    async def test_fetchrow_full(self):
        assert (
            await self.ch.fetchrow("SELECT * FROM all_types WHERE uint8=1")
            == self.rows[0]
        )

    async def test_fetchrow_with_empties(self):
        assert (
            await self.ch.fetchrow("SELECT * FROM all_types WHERE uint8=2")
            == self.rows[1]
        )

    async def test_fetchone_full(self):
        assert (
            await self.ch.fetchone("SELECT * FROM all_types WHERE uint8=1")
            == self.rows[0]
        )

    async def test_fetchone_with_empties(self):
        assert (
            await self.ch.fetchone("SELECT * FROM all_types WHERE uint8=2")
            == self.rows[1]
        )

    async def test_fetch(self):
        assert await self.ch.fetch("SELECT * FROM all_types") == self.rows

    async def test_cursor(self):
        assert [
            row async for row in self.ch.cursor("SELECT * FROM all_types")
        ] == self.rows

    async def test_iterate(self):
        assert [
            row async for row in self.ch.iterate("SELECT * FROM all_types")
        ] == self.rows
