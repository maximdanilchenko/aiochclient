import datetime as dt
import json
import os
from decimal import Decimal
from enum import Enum, IntEnum
from ipaddress import IPv4Address, IPv6Address
from uuid import UUID, uuid4

import aiohttp
import httpx
import pytest

from aiochclient import ChClient, ChClientError

pytestmark = pytest.mark.asyncio


@pytest.fixture
def uuid():
    return uuid4()


@pytest.fixture
def rows(uuid):
    return [
        [
            1,
            1000,
            10000,
            12_345_678_910,
            12_345_678_910_231,
            12_345_678_910_234_432_123,
            -4,
            -453,
            21322,
            -32123,
            12_345_678_910_231,
            12_345_678_910_234_432_123,
            23.432,
            -56754.564_542,
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
            ["hello", "world"],
            ["hello", None],
            [("hello\'", 3, "hello")],
            "'\b\f\r\n\t\\",
            uuid,
            [uuid, uuid, uuid],
            ["hello", "world", "hello"],
            [dt.date(2018, 9, 21), dt.date(2018, 9, 22)],
            [
                dt.datetime(2018, 9, 21, 10, 32, 23),
                dt.datetime(2018, 9, 21, 10, 32, 24),
            ],
            "hello man",
            "hello man",
            777,
            dt.date(1994, 9, 7),
            dt.datetime(2018, 9, 21, 10, 32, 23),
            Decimal('1234.5678'),
            Decimal('1234.56'),
            Decimal('1234.56'),
            Decimal('123.56'),
            [[1, 2, 3], [1, 2], [6, 7]],
            IPv4Address('116.253.40.133'),
            IPv6Address('2001:44c8:129:2632:33:0:252:2'),
            dt.datetime(2018, 9, 21, 10, 32, 23, 999000),
            True,
            {"hello": "world {' and other things"},
            {"hello": {"inner": "world {' and other things"}},
            {'key1': {'key2': [uuid]}},
            [(1, 2), (3, 4)],
            [('hello', dt.date(2018, 9, 21)), ('world', dt.date(2018, 9, 22))],
        ],
        [
            2,
            1000,
            10000,
            12_345_678_910,
            12_345_678_910_231,
            12_345_678_910_234_432_123,
            -4,
            -453,
            21322,
            -32123,
            12_345_678_910_231,
            12_345_678_910_234_432_123,
            23.432,
            -56754.564_542,
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
            [],
            [],
            [],
            "'\b\f\r\n\t\\",
            None,
            [],
            [],
            [],
            [],
            "hello man",
            None,
            777,
            dt.date(1994, 9, 7),
            dt.datetime(2018, 9, 21, 10, 32, 23),
            Decimal('1234.5678'),
            Decimal('1234.56'),
            Decimal('1234.56'),
            Decimal('123.56'),
            [],
            None,
            None,
            dt.datetime(2019, 1, 1, 3, 0),
            False,
            {"hello": "world {'"},
            {"hello": {"inner": "world {'"}},
            {'key1': {'key2': [uuid, uuid, uuid]}},
            [(0, 1)],
            [
                ('hello', dt.date(2018, 9, 21)),
                ('inner', dt.date(2018, 9, 22)),
                ('world', dt.date(2018, 9, 23)),
            ],
        ],
    ]


@pytest.fixture(params=[aiohttp.ClientSession, httpx.AsyncClient])
def http_client(request):
    return request.param


@pytest.fixture(
    params=[
        {
            "compress_response": True,
            "user": "default",
            "password": "",
            "database": "default",
            "allow_suspicious_low_cardinality_types": 1,
            "flatten_nested": 0,
        },
        {
            "allow_suspicious_low_cardinality_types": 1,
            "flatten_nested": 0,
        },
    ]
)
async def chclient(request, http_client):
    async with ChClient(http_client(), **request.param) as chclient:
        yield chclient


@pytest.fixture
async def all_types_db(chclient, rows):
    await chclient.execute("DROP TABLE IF EXISTS all_types")
    await chclient.execute("DROP TABLE IF EXISTS test_cache")
    await chclient.execute("DROP TABLE IF EXISTS test_cache_mv")
    await chclient.execute("DROP TABLE IF EXISTS test_insert_file")
    await chclient.execute("""
    CREATE TABLE all_types (uint8 UInt8,
                            uint16 UInt16,
                            uint32 UInt32,
                            uint64 UInt64,
                            uint128 UInt128,
                            uint256 UInt256,
                            int8 Int8,
                            int16 Int16,
                            int32 Int32,
                            int64 Int64,
                            int128 Int128,
                            int256 Int256,
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
                            array_low_cardinality_string Array(LowCardinality(String)),
                            array_nullable_string Array(Nullable(String)),
                            array_tuple Array(Tuple(String, UInt8, String)),
                            escape_string String,
                            uuid Nullable(UUID),
                            array_uuid Array(UUID),
                            array_enum Array(Enum8('hello' = 1, 'world' = 2)),
                            array_date Array(Date),
                            array_datetime Array(DateTime),
                            low_cardinality_str LowCardinality(String),
                            low_cardinality_nullable_str LowCardinality(Nullable(String)),
                            low_cardinality_int LowCardinality(Int32),
                            low_cardinality_date LowCardinality(Date),
                            low_cardinality_datetime LowCardinality(DateTime),
                            decimal32 Decimal32(4),
                            decimal64 Decimal64(2),
                            decimal128 Decimal128(6),
                            decimal Decimal(6, 3),
                            array_array_int Array(Array(Int32)),
                            ipv4 Nullable(IPv4),
                            ipv6 Nullable(IPv6),
                            datetime64 DateTime64(3, 'Europe/Moscow'),
                            bool Bool,
                            map Map(String, String),
                            map_map Map(String, Map(String, String)),
                            map_map_array_uuid Map(String, Map(String, Array(UUID))),
                            nested_int Nested(value1 Integer, value2 Integer),
                            nested_str_date Nested(value1 String, value2 Date)
                            ) ENGINE = Memory
    """)
    await chclient.execute("""
        CREATE TABLE test_cache (
          key           String,
          int32Cache    AggregateFunction(avg, Int32),
          float32Cache  SimpleAggregateFunction(sum, Float64))
        ENGINE = AggregatingMergeTree()
        ORDER BY key
        """)
    await chclient.execute("""
        CREATE MATERIALIZED VIEW test_cache_mv TO test_cache AS
          SELECT avgState(int32) AS int32Cache, sum(float32) AS float32Cache
          FROM all_types
        """)
    await chclient.execute("""
        CREATE TABLE test_insert_file(
            uint32  UInt32,
            string  String,
            date    Date
        ) ENGINE = Memory
        """)
    await chclient.execute("INSERT INTO all_types VALUES", *rows)


@pytest.fixture
def class_chclient(chclient, all_types_db, rows, request):
    request.cls.ch = chclient
    cls_rows = rows
    cls_rows[1][45] = dt.datetime(
        2019, 1, 1, 3, 0
    )  # DateTime64 always returns datetime type
    request.cls.rows = [tuple(r) for r in cls_rows]


@pytest.fixture(params=["tsv", "binary", "native"])
def class_engine(request, chclient):
    """Run the decoded type checks against each read engine.

    ``self.engine_ch`` is the client whose decoded output is under test; it
    shares ``chclient``'s session. The ``tsv`` case reuses ``chclient`` itself.
    The raw-bytes (``decode=False``) assertions always stay on the TSV client,
    since that path is TSV-only.
    """
    engine = request.param
    request.cls.engine = engine
    if engine == "binary":
        request.cls.engine_ch = ChClient(chclient._http_client._session, binary=True)
    elif engine == "native":
        request.cls.engine_ch = ChClient(chclient._http_client._session, native=True)
    else:
        request.cls.engine_ch = chclient


@pytest.mark.client
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


@pytest.mark.types
@pytest.mark.usefixtures("class_chclient", "class_engine")
class TestTypes:
    # The Native format ships DateTime64 without its timezone, so a tz-aware
    # column comes back as naive UTC and cannot match the tz-aware TSV value.
    NATIVE_UNSUPPORTED = {"datetime64"}

    # The binary engines decode Float32 from its exact 4-byte IEEE-754 value,
    # whereas TSV ships ClickHouse's shorter text rounding (e.g. 23.432 vs
    # 23.43199920654297). The expected values here are the TSV roundings, so
    # this column is only comparable on the TSV engine.
    NON_TSV_REPR = {"float32"}

    def _skip_unsupported(self, field):
        name = field.strip()
        if self.engine != "tsv" and name in self.NON_TSV_REPR:
            pytest.skip(f"'{name}' text rounding is TSV-specific")
        if self.engine == "native" and name in self.NATIVE_UNSUPPORTED:
            pytest.skip(f"Native engine does not decode '{name}' yet")

    async def select_field(self, field):
        self._skip_unsupported(field)
        return await self.engine_ch.fetchval(
            f"SELECT {field} FROM all_types WHERE uint8=1"
        )

    async def select_record(self, field):
        self._skip_unsupported(field)
        return await self.engine_ch.fetchrow(
            f"SELECT {field} FROM all_types WHERE uint8=1"
        )

    async def select_field_bytes(self, field):
        return await self.ch.fetchval(
            f"SELECT {field} FROM all_types WHERE uint8=1", decode=False
        )

    async def select_record_bytes(self, field):
        return await self.ch.fetchrow(
            f"SELECT {field} FROM all_types WHERE uint8=1", decode=False
        )

    async def test_uint8(self):
        result = 1
        assert await self.select_field("uint8") == result
        record = await self.select_record("uint8")
        assert record[0] == result
        assert record["uint8"] == result

        result = b"1"
        assert await self.select_field_bytes("uint8") == result
        record = await self.select_record_bytes("uint8")
        assert record[0] == result
        assert record["uint8"] == result

    async def test_uint16(self):
        result = 1000
        assert await self.select_field("uint16") == result
        record = await self.select_record("uint16")
        assert record[0] == result
        assert record["uint16"] == result

        result = b"1000"
        assert await self.select_field_bytes("uint16") == result
        record = await self.select_record_bytes("uint16")
        assert record[0] == result
        assert record["uint16"] == result

    async def test_uint32(self):
        result = 10000
        assert await self.select_field("uint32") == result
        record = await self.select_record("uint32")
        assert record[0] == result
        assert record["uint32"] == result

        result = b"10000"
        assert await self.select_field_bytes("uint32") == result
        record = await self.select_record_bytes("uint32")
        assert record[0] == result
        assert record["uint32"] == result

    async def test_uint64(self):
        result = 12_345_678_910
        assert await self.select_field("uint64") == result
        record = await self.select_record("uint64")
        assert record[0] == result
        assert record["uint64"] == result

        result = b"12345678910"
        assert await self.select_field_bytes("uint64") == result
        record = await self.select_record_bytes("uint64")
        assert record[0] == result
        assert record["uint64"] == result

    async def test_uint128(self):
        result = 12_345_678_910_231
        assert await self.select_field("uint128") == result
        record = await self.select_record("uint128")
        assert record[0] == result
        assert record["uint128"] == result

        result = b"12345678910231"
        assert await self.select_field_bytes("uint128") == result
        record = await self.select_record_bytes("uint128")
        assert record[0] == result
        assert record["uint128"] == result

    async def test_uint256(self):
        result = 12_345_678_910_234_432_123
        assert await self.select_field("uint256") == result
        record = await self.select_record("uint256")
        assert record[0] == result
        assert record["uint256"] == result

        result = b"12345678910234432123"
        assert await self.select_field_bytes("uint256") == result
        record = await self.select_record_bytes("uint256")
        assert record[0] == result
        assert record["uint256"] == result

    async def test_int8(self):
        result = -4
        assert await self.select_field("int8") == result
        record = await self.select_record("int8")
        assert record[0] == result
        assert record["int8"] == result

        result = b"-4"
        assert await self.select_field_bytes("int8") == result
        record = await self.select_record_bytes("int8")
        assert record[0] == result
        assert record["int8"] == result

    async def test_int16(self):
        result = -453
        assert await self.select_field("int16") == result
        record = await self.select_record("int16")
        assert record[0] == result
        assert record["int16"] == result

        result = b"-453"
        assert await self.select_field_bytes("int16") == result
        record = await self.select_record_bytes("int16")
        assert record[0] == result
        assert record["int16"] == result

    async def test_int32(self):
        result = 21322
        assert await self.select_field("int32") == result
        record = await self.select_record("int32")
        assert record[0] == result
        assert record["int32"] == result

        result = b"21322"
        assert await self.select_field_bytes("int32") == result
        record = await self.select_record_bytes("int32")
        assert record[0] == result
        assert record["int32"] == result

    async def test_int64(self):
        result = -32123
        assert await self.select_field("int64") == result
        record = await self.select_record("int64")
        assert record[0] == result
        assert record["int64"] == result

        result = b"-32123"
        assert await self.select_field_bytes("int64") == result
        record = await self.select_record_bytes("int64")
        assert record[0] == result
        assert record["int64"] == result

    async def test_int128(self):
        result = 12_345_678_910_231
        assert await self.select_field("int128") == result
        record = await self.select_record("int128")
        assert record[0] == result
        assert record["int128"] == result

        result = b"12345678910231"
        assert await self.select_field_bytes("int128") == result
        record = await self.select_record_bytes("int128")
        assert record[0] == result
        assert record["int128"] == result

    async def test_int256(self):
        result = 12_345_678_910_234_432_123
        assert await self.select_field("int256") == result
        record = await self.select_record("int256")
        assert record[0] == result
        assert record["int256"] == result

        result = b"12345678910234432123"
        assert await self.select_field_bytes("int256") == result
        record = await self.select_record_bytes("int256")
        assert record[0] == result
        assert record["int256"] == result

    async def test_float32(self):
        result = 23.432
        assert await self.select_field("float32") == result
        record = await self.select_record("float32")
        assert record[0] == result
        assert record["float32"] == result

        result = b"23.432"
        assert await self.select_field_bytes("float32") == result
        record = await self.select_record_bytes("float32")
        assert record[0] == result
        assert record["float32"] == result

    async def test_float64(self):
        result = -56754.564_542
        assert await self.select_field("float64") == result
        record = await self.select_record("float64")
        assert record[0] == result
        assert record["float64"] == result

        result = b"-56754.564542"
        assert await self.select_field_bytes("float64") == result
        record = await self.select_record_bytes("float64")
        assert record[0] == result
        assert record["float64"] == result

    async def test_string(self):
        result = "hello man"
        assert await self.select_field("string") == result
        record = await self.select_record("string")
        assert record[0] == result
        assert record["string"] == result

        result = b"hello man"
        assert await self.select_field_bytes("string") == result
        record = await self.select_record_bytes("string")
        assert record[0] == result
        assert record["string"] == result

    async def test_fixed_string(self):
        result = "hello fixed man".ljust(32, " ")
        assert await self.select_field("fixed_string") == result
        record = await self.select_record("fixed_string")
        assert record[0] == result
        assert record["fixed_string"] == result

        result = b"hello fixed man".ljust(32, b" ")
        assert await self.select_field_bytes("fixed_string") == result
        record = await self.select_record_bytes("fixed_string")
        assert record[0] == result
        assert record["fixed_string"] == result

    async def test_date(self):
        result = dt.date(2018, 9, 21)
        assert await self.select_field("date") == result
        record = await self.select_record("date")
        assert record[0] == result
        assert record["date"] == result

        result = b"2018-09-21"
        assert await self.select_field_bytes("date") == result
        record = await self.select_record_bytes("date")
        assert record[0] == result
        assert record["date"] == result

    async def test_datetime(self):
        result = dt.datetime(2018, 9, 21, 10, 32, 23)
        assert await self.select_field("datetime") == result
        record = await self.select_record("datetime")
        assert record[0] == result
        assert record["datetime"] == result

        result = b"2018-09-21 10:32:23"
        assert await self.select_field_bytes("datetime") == result
        record = await self.select_record_bytes("datetime")
        assert record[0] == result
        assert record["datetime"] == result

    async def test_enum8(self):
        result = "hello"
        assert await self.select_field("enum8") == result
        record = await self.select_record("enum8")
        assert record[0] == result
        assert record["enum8"] == result

        result = b"hello"
        assert await self.select_field_bytes("enum8") == result
        record = await self.select_record_bytes("enum8")
        assert record[0] == result
        assert record["enum8"] == result

    async def test_enum16(self):
        result = "world"
        assert await self.select_field("enum16") == result
        record = await self.select_record("enum16")
        assert record[0] == result
        assert record["enum16"] == result

        result = b"world"
        assert await self.select_field_bytes("enum16") == result
        record = await self.select_record_bytes("enum16")
        assert record[0] == result
        assert record["enum16"] == result

    async def test_array_uint8(self):
        result = [1, 2, 3, 4]
        assert await self.select_field("array_uint8") == result
        record = await self.select_record("array_uint8")
        assert record[0] == result
        assert record["array_uint8"] == result

        result = b"[1,2,3,4]"
        assert await self.select_field_bytes("array_uint8") == result
        record = await self.select_record_bytes("array_uint8")
        assert record[0] == result
        assert record["array_uint8"] == result

    async def test_nested_int(self):
        result = [(1, 2), (3, 4)]
        assert await self.select_field("nested_int") == result
        record = await self.select_record("nested_int")
        assert record[0] == result
        assert record["nested_int"] == result

        result = b'[(1,2),(3,4)]'
        assert await self.select_field_bytes("nested_int") == result
        record = await self.select_record_bytes("nested_int")
        assert record[0] == result
        assert record["nested_int"] == result

    async def test_nested_string_date(self):
        result = [('hello', dt.date(2018, 9, 21)), ('world', dt.date(2018, 9, 22))]
        assert await self.select_field("nested_str_date") == result
        record = await self.select_record("nested_str_date")
        assert record[0] == result
        assert record["nested_str_date"] == result

        result = b"[('hello','2018-09-21'),('world','2018-09-22')]"
        assert await self.select_field_bytes("nested_str_date") == result
        record = await self.select_record_bytes("nested_str_date")
        assert record[0] == result
        assert record["nested_str_date"] == result

    async def test_tuple(self):
        result = (4, "hello")
        assert await self.select_field("tuple") == result
        record = await self.select_record("tuple")
        assert record[0] == result
        assert record["tuple"] == result

        result = b"(4,'hello')"
        assert await self.select_field_bytes("tuple") == result
        record = await self.select_record_bytes("tuple")
        assert record[0] == result
        assert record["tuple"] == result

    async def test_map(self):
        result = {"hello": "world {' and other things"}
        assert await self.select_field("map") == result
        record = await self.select_record("map")
        assert record[0] == result
        assert record["map"] == result

        result = b"{'hello':'world {\\' and other things'}"
        assert await self.select_field_bytes("map") == result
        record = await self.select_record_bytes("map")
        assert record[0] == result
        assert record["map"] == result

    async def test_map_map(self):
        result = {"hello": {"inner": "world {' and other things"}}
        assert await self.select_field("map_map") == result
        record = await self.select_record("map_map")
        assert record[0] == result
        assert record["map_map"] == result

        result = b"{'hello':{'inner':'world {\\' and other things'}}"
        assert await self.select_field_bytes("map_map") == result
        record = await self.select_record_bytes("map_map")
        assert record[0] == result
        assert record["map_map"] == result

    async def test_map_map_array_uuid(self, uuid):
        result = {'key1': {'key2': [uuid]}}
        print(await self.select_field("map_map_array_uuid"))
        assert await self.select_field("map_map_array_uuid") == result
        record = await self.select_record("map_map_array_uuid")
        assert record[0] == result
        assert record["map_map_array_uuid"] == result

        result = ("{'key1':{'key2':" f"['{str(uuid)}']" "}}").encode()
        print(await self.select_field_bytes("map_map_array_uuid"))
        assert await self.select_field_bytes("map_map_array_uuid") == result
        record = await self.select_record_bytes("map_map_array_uuid")
        assert record[0] == result
        assert record["map_map_array_uuid"] == result

    async def test_nullable(self):
        result = 0
        assert await self.select_field("nullable") == result
        record = await self.select_record("nullable")
        assert record[0] == result
        assert record["nullable"] == result

        result = b"0"
        assert await self.select_field_bytes("nullable") == result
        record = await self.select_record_bytes("nullable")
        assert record[0] == result
        assert record["nullable"] == result

    async def test_array_string(self):
        result = ["hello", "world"]
        assert await self.select_field("array_string") == result
        record = await self.select_record("array_string")
        assert record[0] == result
        assert record["array_string"] == result

        result = b"['hello','world']"
        assert await self.select_field_bytes("array_string") == result
        record = await self.select_record_bytes("array_string")
        assert record[0] == result
        assert record["array_string"] == result

    async def test_array_tuple(self):
        result = [("hello'", 3, "hello")]
        assert await self.select_field("array_tuple") == result
        record = await self.select_record("array_tuple")
        assert record[0] == result
        assert record["array_tuple"] == result

        result = b"[('hello\\'',3,'hello')]"
        assert await self.select_field_bytes("array_tuple") == result
        record = await self.select_record_bytes("array_tuple")
        assert record[0] == result
        assert record["array_tuple"] == result

    async def test_array_low_cardinality_string(self):
        result = ["hello", "world"]
        assert await self.select_field("array_low_cardinality_string") == result
        record = await self.select_record("array_low_cardinality_string")
        assert record[0] == result
        assert record["array_low_cardinality_string"] == result

        result = b"['hello','world']"
        assert await self.select_field_bytes("array_low_cardinality_string") == result
        record = await self.select_record_bytes("array_low_cardinality_string")
        assert record[0] == result
        assert record["array_low_cardinality_string"] == result

    async def test_array_nullable_string(self):
        result = ["hello", None]
        assert await self.select_field("array_nullable_string") == result
        record = await self.select_record("array_nullable_string")
        assert record[0] == result
        assert record["array_nullable_string"] == result

        result = b"['hello',NULL]"
        assert await self.select_field_bytes("array_nullable_string") == result
        record = await self.select_record_bytes("array_nullable_string")
        assert record[0] == result
        assert record["array_nullable_string"] == result

    async def test_escape_string(self):
        result = "'\b\f\r\n\t\\"
        assert await self.select_field("escape_string") == result
        record = await self.select_record("escape_string")
        assert record[0] == result
        assert record["escape_string"] == result

        result = b"\\'\\b\\f\\r\\n\\t\\\\"
        assert await self.select_field_bytes("escape_string") == result
        record = await self.select_record_bytes("escape_string")
        assert record[0] == result
        assert record["escape_string"] == result

    async def test_uuid(self, uuid):
        result = uuid
        assert await self.select_field("uuid") == result
        record = await self.select_record("uuid")
        assert record[0] == result
        assert record["uuid"] == result

        result = str(uuid).encode()
        assert await self.select_field_bytes("uuid") == result
        record = await self.select_record_bytes("uuid")
        assert record[0] == result
        assert record["uuid"] == result

    async def test_array_uuid(self, uuid):
        result = [uuid, uuid, uuid]
        assert await self.select_field("array_uuid") == result
        record = await self.select_record("array_uuid")
        assert record[0] == result
        assert record["array_uuid"] == result

        result = str([str(uuid), str(uuid), str(uuid)]).replace(" ", "").encode()
        assert await self.select_field_bytes("array_uuid") == result
        record = await self.select_record_bytes("array_uuid")
        assert record[0] == result
        assert record["array_uuid"] == result

    async def test_array_enum(self):
        result = ["hello", "world", "hello"]
        assert await self.select_field("array_enum ") == result
        record = await self.select_record("array_enum ")
        assert record[0] == result
        assert record["array_enum"] == result

        result = b"['hello','world','hello']"
        assert await self.select_field_bytes("array_enum ") == result
        record = await self.select_record_bytes("array_enum ")
        assert record[0] == result
        assert record["array_enum"] == result

    async def test_array_date(self):
        assert await self.select_field("array_date ") == [
            dt.date(2018, 9, 21),
            dt.date(2018, 9, 22),
        ]
        assert await self.select_field_bytes("array_date ") == (
            b"['2018-09-21','2018-09-22']"
        )

    async def test_array_datetime(self):
        assert await self.select_field("array_datetime ") == [
            dt.datetime(2018, 9, 21, 10, 32, 23),
            dt.datetime(2018, 9, 21, 10, 32, 24),
        ]
        assert await self.select_field_bytes("array_datetime ") == (
            b"['2018-09-21 10:32:23','2018-09-21 10:32:24']"
        )

    async def test_low_cardinality_str(self):
        result = "hello man"
        assert await self.select_field("low_cardinality_str") == result
        record = await self.select_record("low_cardinality_str")
        assert record[0] == result
        assert record["low_cardinality_str"] == result

        result = b"hello man"
        assert await self.select_field_bytes("low_cardinality_str") == result
        record = await self.select_record_bytes("low_cardinality_str")
        assert record[0] == result
        assert record["low_cardinality_str"] == result

    async def test_low_cardinality_nullable_str(self):
        result = "hello man"
        assert await self.select_field("low_cardinality_nullable_str") == result
        record = await self.select_record("low_cardinality_nullable_str")
        assert record[0] == result
        assert record["low_cardinality_nullable_str"] == result

        result = b"hello man"
        assert await self.select_field_bytes("low_cardinality_nullable_str") == result
        record = await self.select_record_bytes("low_cardinality_nullable_str")
        assert record[0] == result
        assert record["low_cardinality_nullable_str"] == result

    async def test_low_cardinality_int(self):
        result = 777
        assert await self.select_field("low_cardinality_int") == result
        record = await self.select_record("low_cardinality_int")
        assert record[0] == result
        assert record["low_cardinality_int"] == result

        result = b"777"
        assert await self.select_field_bytes("low_cardinality_int") == result
        record = await self.select_record_bytes("low_cardinality_int")
        assert record[0] == result
        assert record["low_cardinality_int"] == result

    async def test_low_cardinality_date(self):
        result = dt.date(1994, 9, 7)
        assert await self.select_field("low_cardinality_date") == result
        record = await self.select_record("low_cardinality_date")
        assert record[0] == result
        assert record["low_cardinality_date"] == result

        result = b"1994-09-07"
        assert await self.select_field_bytes("low_cardinality_date") == result
        record = await self.select_record_bytes("low_cardinality_date")
        assert record[0] == result
        assert record["low_cardinality_date"] == result

    async def test_low_cardinality_datetime(self):
        assert await self.select_field("low_cardinality_datetime") == dt.datetime(
            2018, 9, 21, 10, 32, 23
        )

        assert (
            await self.select_field_bytes("low_cardinality_datetime")
            == b"2018-09-21 10:32:23"
        )

    async def test_decimal(self):
        assert await self.select_field("decimal") == Decimal("123.56")

        assert await self.select_field_bytes("decimal") == b"123.56"

    async def test_decimal32(self):
        assert await self.select_field("decimal32") == Decimal("1234.5678")

        assert await self.select_field_bytes("decimal32") == b"1234.5678"

    async def test_decimal64(self):
        assert await self.select_field("decimal64") == Decimal("1234.56")

        assert await self.select_field_bytes("decimal64") == b"1234.56"

    async def test_decimal128(self):
        assert await self.select_field("decimal128") == Decimal("1234.56")

        assert await self.select_field_bytes("decimal128") == b"1234.56"

    async def test_array_of_arrays(self):
        assert await self.select_field("array_array_int") == [[1, 2, 3], [1, 2], [6, 7]]

        assert (
            await self.select_field_bytes("array_array_int") == b"[[1,2,3],[1,2],[6,7]]"
        )

    async def test_ipv4(self):
        assert await self.select_field("ipv4") == IPv4Address("116.253.40.133")

        assert await self.select_field_bytes("ipv4") == b"116.253.40.133"

    async def test_ipv6(self):
        assert await self.select_field("ipv6") == IPv6Address(
            '2001:44c8:129:2632:33:0:252:2'
        )

        assert await self.select_field_bytes("ipv6") == b"2001:44c8:129:2632:33:0:252:2"

    async def test_datetime64(self):
        result = dt.datetime(2018, 9, 21, 10, 32, 23, 999000)
        assert await self.select_field("datetime64") == result
        record = await self.select_record("datetime64")
        assert record[0] == result
        assert record["datetime64"] == result

        result = b"2018-09-21 10:32:23.999"
        assert await self.select_field_bytes("datetime64") == result
        record = await self.select_record_bytes("datetime64")
        assert record[0] == result, record
        assert record["datetime64"] == result, record

    async def test_datetime64_nanoseconds(self):
        # https://github.com/maximdanilchenko/aiochclient/issues/91
        # https://github.com/maximdanilchenko/aiochclient/issues/127
        # DateTime64(9) carries nanoseconds, but Python datetime only supports
        # microseconds — the fractional part is truncated to 6 digits, and
        # consistently so with or without ciso8601 installed.
        await self.ch.execute("DROP TABLE IF EXISTS dt64_ns")
        await self.ch.execute("CREATE TABLE dt64_ns (d DateTime64(9)) ENGINE = Memory")
        await self.ch.execute(
            "INSERT INTO dt64_ns VALUES ('2024-06-24 19:42:52.123456789')"
        )
        assert await self.ch.fetchval("SELECT d FROM dt64_ns") == dt.datetime(
            2024, 6, 24, 19, 42, 52, 123456
        )
        await self.ch.execute("DROP TABLE IF EXISTS dt64_ns")

    async def test_insert_datetime_with_microseconds(self):
        # https://github.com/maximdanilchenko/aiochclient/issues/119
        # A datetime carrying microseconds inserts into a DateTime column (the
        # server truncates to second precision) ...
        await self.ch.execute("DROP TABLE IF EXISTS dt_micro")
        await self.ch.execute("CREATE TABLE dt_micro (d DateTime) ENGINE = Memory")
        await self.ch.execute(
            "INSERT INTO dt_micro VALUES",
            (dt.datetime(2024, 6, 24, 19, 42, 52, 607030),),
        )
        assert await self.ch.fetchval("SELECT d FROM dt_micro") == dt.datetime(
            2024, 6, 24, 19, 42, 52
        )
        # ... and the same value keeps its sub-second part in a DateTime64
        # column — which is why the client always sends the microseconds.
        await self.ch.execute("DROP TABLE IF EXISTS dt64_micro")
        await self.ch.execute(
            "CREATE TABLE dt64_micro (d DateTime64(6)) ENGINE = Memory"
        )
        await self.ch.execute(
            "INSERT INTO dt64_micro VALUES",
            (dt.datetime(2024, 6, 24, 19, 42, 52, 607030),),
        )
        assert await self.ch.fetchval("SELECT d FROM dt64_micro") == dt.datetime(
            2024, 6, 24, 19, 42, 52, 607030
        )
        await self.ch.execute("DROP TABLE IF EXISTS dt_micro")
        await self.ch.execute("DROP TABLE IF EXISTS dt64_micro")

    async def test_named_tuples(self):
        """Named tuples are used for example in geohash functions

        https://clickhouse.com/docs/en/sql-reference/data-types/tuple/#addressing-tuple-elements
        """

        result = await self.ch.fetchval(
            f"SELECT (1.0, 2.0)::Tuple(x Float64, y Float64)"
        )
        assert round(result[0]) == 1
        assert round(result[1]) == 2

    async def test_array_tuple_with_special_chars_in_string(self):
        # https://github.com/maximdanilchenko/aiochclient/issues/123
        # A string element containing structural characters (parens, comma,
        # quote) must not confuse the tuple/array parser.
        value = [
            ("key1", "value1"),
            ("key2", "an invalid pair(with some parens) and this, let's see"),
        ]
        await self.ch.execute("DROP TABLE IF EXISTS t_issue_123")
        await self.ch.execute(
            "CREATE TABLE t_issue_123 (a Array(Tuple(String, String))) ENGINE = Memory"
        )
        await self.ch.execute("INSERT INTO t_issue_123 VALUES", [value])
        assert await self.ch.fetchval("SELECT a FROM t_issue_123") == value
        await self.ch.execute("DROP TABLE IF EXISTS t_issue_123")

    async def test_map_with_multiple_entries(self):
        # https://github.com/maximdanilchenko/aiochclient/issues/118
        value = {"a": 1, "b": 2, "c": 3}
        await self.ch.execute("DROP TABLE IF EXISTS t_issue_118")
        await self.ch.execute(
            "CREATE TABLE t_issue_118 (m Map(String, UInt64)) ENGINE = Memory"
        )
        await self.ch.execute("INSERT INTO t_issue_118 VALUES", [value])
        assert await self.ch.fetchval("SELECT m FROM t_issue_118") == value
        await self.ch.execute("DROP TABLE IF EXISTS t_issue_118")

    async def test_map_with_separators_in_string_values(self):
        # https://github.com/maximdanilchenko/aiochclient/issues/118
        # Commas and colons inside string keys/values must not be treated
        # as map separators.
        value = {"k,1": "a,b:c", "k:2": "plain"}
        await self.ch.execute("DROP TABLE IF EXISTS t_issue_118_sep")
        await self.ch.execute(
            "CREATE TABLE t_issue_118_sep (m Map(String, String)) ENGINE = Memory"
        )
        await self.ch.execute("INSERT INTO t_issue_118_sep VALUES", [value])
        assert await self.ch.fetchval("SELECT m FROM t_issue_118_sep") == value
        await self.ch.execute("DROP TABLE IF EXISTS t_issue_118_sep")

    async def test_empty_map(self):
        # https://github.com/maximdanilchenko/aiochclient/issues/117
        await self.ch.execute("DROP TABLE IF EXISTS t_issue_117")
        await self.ch.execute(
            "CREATE TABLE t_issue_117 (m Map(String, UInt64)) ENGINE = Memory"
        )
        await self.ch.execute("INSERT INTO t_issue_117 VALUES", [{}])
        assert await self.ch.fetchval("SELECT m FROM t_issue_117") == {}
        await self.ch.execute("DROP TABLE IF EXISTS t_issue_117")

    async def test_insert_subtypes(self):
        # https://github.com/maximdanilchenko/aiochclient/issues/97
        # Subclasses of supported types (e.g. str/int Enums) must be accepted.
        class Color(str, Enum):
            RED = "red"

        class Level(IntEnum):
            HIGH = 5

        await self.ch.execute("DROP TABLE IF EXISTS t_issue_97")
        await self.ch.execute(
            "CREATE TABLE t_issue_97 (s String, n UInt8) ENGINE = Memory"
        )
        await self.ch.execute("INSERT INTO t_issue_97 VALUES", (Color.RED, Level.HIGH))
        assert await self.ch.fetchrow("SELECT s, n FROM t_issue_97") == {
            "s": "red",
            "n": 5,
        }
        await self.ch.execute("DROP TABLE IF EXISTS t_issue_97")


@pytest.mark.fetching
@pytest.mark.usefixtures("class_chclient")
class TestFetching:
    async def test_fetchrow_full(self):
        assert (await self.ch.fetchrow("SELECT * FROM all_types WHERE uint8=1"))[
            :
        ] == self.rows[0]

    async def test_fetchrow_full_with_params(self):
        assert (
            await self.ch.fetchrow(
                "SELECT * FROM all_types WHERE uint8={u8}", params={"u8": 1}
            )
        )[:] == self.rows[0]

    async def test_fetchrow_with_empties(self):
        assert (await self.ch.fetchrow("SELECT * FROM all_types WHERE uint8=2"))[
            :
        ] == self.rows[1]

    async def test_fetchrow_none_result(self):
        assert (
            await self.ch.fetchrow("SELECT * FROM all_types WHERE uint8=42")
        ) is None

    async def test_fetchrow_none_result_with_params(self):
        assert (
            await self.ch.fetchrow(
                "SELECT * FROM all_types WHERE uint8={u8}", params={'u8': 42}
            )
        ) is None

    async def test_fetchone_full(self):
        assert (await self.ch.fetchone("SELECT * FROM all_types WHERE uint8=1"))[
            :
        ] == self.rows[0]

    async def test_fetchone_with_empties(self):
        assert (await self.ch.fetchone("SELECT * FROM all_types WHERE uint8=2"))[
            :
        ] == self.rows[1]

    async def test_fetchone_none_result(self):
        assert (
            await self.ch.fetchone("SELECT * FROM all_types WHERE uint8=42")
        ) is None

    async def test_fetchval_none_result(self):
        assert (
            await self.ch.fetchval("SELECT uint8 FROM all_types WHERE uint8=42")
        ) is None

    async def test_fetchval_none_result_with_params(self):
        assert (
            await self.ch.fetchval(
                "SELECT uint8 FROM all_types WHERE uint8={u8}", params={"u8": 42}
            )
        ) is None

    async def test_fetch(self):
        rows = await self.ch.fetch("SELECT * FROM all_types")
        assert [row[:] for row in rows] == self.rows

    async def test_cursor(self):
        assert [
            row[:] async for row in self.ch.cursor("SELECT * FROM all_types")
        ] == self.rows

    async def test_iterate(self):
        assert [
            row[:] async for row in self.ch.iterate("SELECT * FROM all_types")
        ] == self.rows

    async def test_select_with_execute(self):
        assert (await self.ch.execute("SELECT * FROM all_types WHERE uint8=1")) is None

    async def test_describe_with_fetch(self):
        described_columns = await self.ch.fetch("DESCRIBE TABLE all_types", json=True)
        assert described_columns is not None
        assert 'type' in described_columns[0]
        assert 'name' in described_columns[0]

    async def test_show_tables_with_fetch(self):
        tables = await self.ch.fetch("SHOW TABLES")
        assert len(tables) == 4
        assert tables[0]._row.decode() == 'all_types'

    async def test_aggr_merge_tree(self):
        avg_value = await self.ch.execute("SELECT avg(int32) FROM all_types")
        avg_cache = await self.ch.execute("SELECT avgMerge(int32Cache) FROM test_cache")
        assert avg_value == avg_cache

    async def test_exists_table(self):
        exists = await self.ch.fetchrow("EXISTS TABLE all_types")
        assert exists == {'result': 1}

    async def test_explain_with_fetch(self):
        # https://github.com/maximdanilchenko/aiochclient/issues/98
        rows = await self.ch.fetch("EXPLAIN SELECT 1")
        assert rows
        assert all(isinstance(row[0], str) for row in rows)

        value = await self.ch.fetchval("EXPLAIN SYNTAX SELECT 1 + 1")
        assert value == "SELECT 1 + 1"

    async def test_quoted_string(self):
        record = await self.ch.fetchrow("SELECT 'foo\\'bar' AS quoted_string")
        assert record == {'quoted_string': "foo'bar"}

    async def test_quoted_string_array(self):
        record = await self.ch.fetchrow(
            "SELECT ['foo\\'foo', 'bar', 'foo\\\\'] as array"
        )
        assert record == {'array': ["foo'foo", 'bar', 'foo\\']}

    async def test_quoted_string_tuple(self):
        record = await self.ch.fetchrow("SELECT ('foo\\'foo', 'bar') as tuple")
        assert record == {
            'tuple': (
                "foo'foo",
                'bar',
            )
        }

    async def test_quoted_string_map(self):
        record = await self.ch.fetchrow("SELECT map('foo\\'foo', 'bar\\'bar') as map")
        assert record == {'map': {"foo'foo": "bar'bar"}}

    async def test_no_params(self):
        """It should be possible to have the aliases we want if we don't use any params"""
        res = await self.ch.fetchrow('SELECT 1 AS "{not_a_param}" FROM all_types')
        assert res["{not_a_param}"] == 1


@pytest.mark.record
@pytest.mark.usefixtures("class_chclient")
class TestRecord:
    async def test_common_objects(self):
        records = await self.ch.fetch("SELECT * FROM all_types")
        assert id(records[0]._converters) == id(records[1]._converters)
        assert id(records[0]._names) == id(records[1]._names)

    async def test_lazy_decoding(self):
        record = await self.ch.fetchrow("SELECT * FROM all_types WHERE uint8=2")
        assert type(record._row) == bytes
        record[0]
        assert type(record._row) == tuple
        assert type(record._row[0]) == int

    async def test_mapping(self):
        record = await self.ch.fetchrow("SELECT * FROM all_types WHERE uint8=2")
        assert list(record.values())[0] == 2
        assert list(record.keys())[0] == "uint8"
        assert list(record.items())[0] == ("uint8", 2)
        assert record.get("uint8") == 2
        assert record.get(0) == 2

    async def test_bool(self):
        records = await self.ch.fetch(
            "SELECT uniq(array_string) FROM all_types GROUP BY array_string WITH TOTALS"
        )
        assert bool(records[-2]) is False

    async def test_len(self):
        record = await self.ch.fetchrow("SELECT * FROM all_types WHERE uint8=2")
        assert len(record) == len(self.rows[1])

    async def test_index_error(self):
        record = await self.ch.fetchrow("SELECT * FROM all_types WHERE uint8=2")
        with pytest.raises(IndexError):
            record[len(self.rows[1])]
        records = await self.ch.fetch(
            "SELECT uniq(array_string) FROM all_types GROUP BY array_string WITH TOTALS"
        )
        with pytest.raises(IndexError):
            records[-2][0]

    @pytest.mark.skip
    async def test_empty_string(self):
        await self.ch.execute("INSERT INTO all_types (uint8, string) VALUES", (6, ''))
        result = await self.ch.fetch("SELECT string FROM all_types WHERE uint8=6")
        assert len(result) == 1
        record = result[0]
        assert record['string'] == ''

    async def test_key_error(self):
        record = await self.ch.fetchrow("SELECT * FROM all_types WHERE uint8=2")
        with pytest.raises(KeyError):
            record["no_such_key"]
        records = await self.ch.fetch(
            "SELECT uniq(array_string) FROM all_types GROUP BY array_string WITH TOTALS"
        )
        with pytest.raises(KeyError):
            records[-2]["a"]


@pytest.mark.usefixtures("class_chclient")
class TestJson:
    async def test_json_insert_select(self):
        sql = "INSERT INTO all_types FORMAT JSONEachRow"
        records = [
            {"decimal32": 32},
        ]
        await self.ch.execute(sql, *records)

        sql = "INSERT INTO all_types"
        records = [
            {"fixed_string": "simple string", "low_cardinality_str": "meow test"},
        ]
        await self.ch.execute(sql, *records, json=True)

        result = await self.ch.fetch(
            "SELECT * FROM all_types WHERE decimal32 = 32 FORMAT JSONEachRow"
        )
        assert len(result) == 1
        result = await self.ch.fetch(
            "SELECT fixed_string, low_cardinality_str FROM all_types "
            "WHERE low_cardinality_str = 'meow test'",
            json=True,
        )
        assert result == [
            {
                "fixed_string": "simple string\x00\x00\x00\x00\x00\x00\x00\x00"
                "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",
                "low_cardinality_str": "meow test",
            }
        ]

    async def test_map_map_array_uuid_json(self, uuid):
        result = await self.ch.fetch(
            "SELECT map_map_array_uuid FROM all_types WHERE has(nested_int.value1, 0) format JSONEachRow"
        )
        assert result[0]['map_map_array_uuid'] == {
            'key1': {'key2': [str(uuid), str(uuid), str(uuid)]}
        }

    async def test_select_nested_json(self):
        result = await self.ch.fetch(
            "SELECT nested_int, nested_str_date FROM all_types WHERE has(nested_int.value1, 0) format JSONEachRow"
        )
        assert result[0]['nested_int'] == [{'value1': 0, 'value2': 1}]
        assert result[0]['nested_str_date'] == [
            {'value1': 'hello', 'value2': '2018-09-21'},
            {'value1': 'inner', 'value2': '2018-09-22'},
            {'value1': 'world', 'value2': '2018-09-23'},
        ]


@pytest.mark.usefixtures("class_chclient")
class TestInsertFile:
    async def test_insert_csv_file(self):
        # setup
        data: str = """uint32,string,date
                        1,test,2024-01-03
                        2,hello world,2023-03-10
                        123,test string,2024-01-03"""
        with open('test_data.csv', 'w') as f:
            f.write(data)

        # assert
        with open('test_data.csv', 'rb') as f:
            await self.ch.insert_file(
                'INSERT INTO test_insert_file FORMAT CSV',
                f.read(),
            )
        result = await self.ch.fetch(
            "SELECT * FROM test_insert_file FORMAT JSONEachRow"
        )
        print(result)
        assert result == [
            {'uint32': 1, 'string': 'test', 'date': '2024-01-03'},
            {'uint32': 2, 'string': 'hello world', 'date': '2023-03-10'},
            {'uint32': 123, 'string': 'test string', 'date': '2024-01-03'},
        ]

        # clean
        os.remove('test_data.csv')

    async def test_insert_json_file(self):
        # setup
        data = [
            {"uint32": 1, "string": "test", "date": "2024-01-03"},
            {"uint32": 2, "string": "hello world", "date": "2023-03-10"},
            {"uint32": 3, "string": "", "date": "2018-09-21"},
            {"uint32": 123, "string": "test string", "date": "2024-01-03"},
        ]

        with open('test_data.json', 'w') as f:
            f.write(json.dumps(data))

        # assert
        with open('test_data.json', 'rb') as f:
            await self.ch.insert_file(
                "INSERT INTO test_insert_file FORMAT JSONEachRow",
                f.read(),
            )
        result = await self.ch.fetch("SELECT * FROM test_insert_file")
        assert [row[:] for row in result] == [
            (1, 'test', dt.date(2024, 1, 3)),
            (2, 'hello world', dt.date(2023, 3, 10)),
            (3, '', dt.date(2018, 9, 21)),
            (123, 'test string', dt.date(2024, 1, 3)),
        ]

        # clean
        os.remove('test_data.json')

    async def test_insert_tsv_file(self):
        # setup
        data = (
            "uint32	string	date\n"
            "1	some test string	2024-01-03\n"
            "1	test	2024-01-03\n"
            "1	test	2024-01-03\n"
            "2	hello world	2023-03-10\n"
            "2	hello world	2023-03-10\n"
            "2	hello world	2023-03-10\n"
            "123	other things	2023-03-10\n"
            "123	test string	2024-01-03"
        )
        with open('test_data.tsv', 'w') as f:
            f.write(data)

        # assert
        with open('test_data.tsv', 'rb') as f:
            await self.ch.insert_file(
                "INSERT INTO test_insert_file FORMAT TabSeparated",
                f.read(),
            )
        result = await self.ch.fetch("SELECT * FROM test_insert_file")
        assert [row[:] for row in result] == [
            (1, 'some test string', dt.date(2024, 1, 3)),
            (1, 'test', dt.date(2024, 1, 3)),
            (1, 'test', dt.date(2024, 1, 3)),
            (2, 'hello world', dt.date(2023, 3, 10)),
            (2, 'hello world', dt.date(2023, 3, 10)),
            (2, 'hello world', dt.date(2023, 3, 10)),
            (123, 'other things', dt.date(2023, 3, 10)),
            (123, 'test string', dt.date(2024, 1, 3)),
        ]

        # clean
        os.remove('test_data.tsv')

    async def test_insert_file_with_invalid_format(self):
        # setup
        data: str = (
            "uint32,string,date\n"
            "1,test,2024-01-03\n"
            "2,hello world,2023-03-10\n"
            "123,test string,2024-01-03"
        )
        with open('test_data.csv', 'w') as f:
            f.write(data)

        # assert
        with open('test_data.csv', 'rb') as f:
            with pytest.raises(ChClientError):
                await self.ch.insert_file(
                    'INSERT INTO test_insert_file FORMAT TabSeparated',
                    f.read(),
                )
        # clean
        os.remove('test_data.csv')


class TestRowBinaryDecode:
    # https://github.com/maximdanilchenko/aiochclient/issues/134
    # Golden value <-> bytes vectors captured from a live ClickHouse server,
    # pinning the exact RowBinary wire format. No database needed.
    GOLDEN = [
        ("UInt8", "ff", 255),
        ("UInt16", "0201", 258),
        ("Int32", "feffffff", -2),
        ("UInt64", "0100000000000000", 1),
        ("Int128", "ff" * 16, -1),
        ("Float32", "0000c03f", 1.5),
        ("Float64", "000000000000f83f", 1.5),
        ("Bool", "01", True),
        ("String", "026869", "hi"),
        ("String", "00", ""),
        ("FixedString(4)", "61620000", "ab\x00\x00"),
        ("Date", "0100", dt.date(1970, 1, 2)),
        ("DateTime", "01000000", dt.datetime(1970, 1, 1, 0, 0, 1)),
        ("DateTime64(3)", "6304000000000000", dt.datetime(1970, 1, 1, 0, 0, 1, 123000)),
        ("DateTime64(9)", "7b00000000000000", dt.datetime(1970, 1, 1, 0, 0, 0, 0)),
        ("Decimal(9, 2)", "7b000000", Decimal("1.23")),
        ("Decimal(18, 4)", "3930000000000000", Decimal("1.2345")),
        ("Decimal(38, 2)", "6affffffffffffffffffffffffffffff", Decimal("-1.50")),
        (
            "UUID",
            "3843a816977ca41e44493764f4667e87",
            UUID("1ea47c97-16a8-4338-877e-66f464374944"),
        ),
        ("IPv4", "8528fd74", IPv4Address("116.253.40.133")),
        (
            "IPv6",
            "200144c8012926320033000002520002",
            IPv6Address("2001:44c8:129:2632:33:0:252:2"),
        ),
        ("Enum8('a' = 1, 'b' = 2)", "02", "b"),
        ("Nullable(UInt8)", "01", None),
        ("Nullable(UInt8)", "0005", 5),
        ("Array(UInt8)", "03010203", [1, 2, 3]),
        ("Array(UInt8)", "00", []),
        ("Array(String)", "020161026262", ["a", "bb"]),
        ("Array(Array(UInt8))", "020201020103", [[1, 2], [3]]),
        ("Array(Nullable(UInt8))", "030001010003", [1, None, 3]),
        ("Tuple(UInt8, String)", "04026869", (4, "hi")),
        ("Map(String, UInt8)", "02016101016202", {"a": 1, "b": 2}),
        ("Map(String, UInt8)", "00", {}),
        ("LowCardinality(String)", "026869", "hi"),
    ]

    async def test_golden_vectors(self):
        from aiochclient.binary import Cursor
        from aiochclient.types import what_py_type

        for type_name, hex_bytes, expected in self.GOLDEN:
            value = what_py_type(type_name).read(Cursor(bytes.fromhex(hex_bytes)))
            assert value == expected, type_name
            assert type(value) is type(expected), type_name

    async def test_golden_encode(self):
        from aiochclient.types import what_py_type

        for type_name, hex_bytes, value in self.GOLDEN:
            # DateTime64(9) nanoseconds are truncated to microseconds on read,
            # so that one value is not byte-for-byte round-trippable.
            if "DateTime64(9)" in type_name:
                continue
            assert what_py_type(type_name).write(value).hex() == hex_bytes, type_name

    async def test_varint_boundaries(self):
        from aiochclient.binary import Cursor

        for raw, expected in [("7f", 127), ("8001", 128), ("ac02", 300)]:
            assert Cursor(bytes.fromhex(raw)).read_varint() == expected

    async def test_streaming_across_chunk_boundaries(self):
        # A RowBinaryWithNamesAndTypes body fed one byte at a time must still
        # reassemble into the right record (buffer-refill / re-parse path).
        from aiochclient.binary import rows_from_binary

        body = bytes.fromhex(
            "02"  # 2 columns
            "0161"  # name "a"
            "026262"  # name "bb"
            "0555496e7438"  # type "UInt8"
            "06537472696e67"  # type "String"
            "01"  # row: a = 1
            "026869"  # row: bb = "hi"
        )

        async def one_byte_at_a_time():
            for i in range(len(body)):
                yield body[i : i + 1]

        records = [r async for r in rows_from_binary(one_byte_at_a_time())]
        assert len(records) == 1
        assert records[0]["a"] == 1
        assert records[0]["bb"] == "hi"


@pytest.mark.usefixtures("class_chclient")
class TestRowBinary:
    # https://github.com/maximdanilchenko/aiochclient/issues/134
    def _binary_client(self):
        return ChClient(self.ch._http_client._session, binary=True)

    @staticmethod
    def _eq(tsv_value, bin_value):
        # Float32 text (TSV) and the exact float32 bits (RowBinary) differ when
        # widened to float64, so floats are compared with a tolerance.
        if isinstance(tsv_value, float):
            return bin_value == pytest.approx(tsv_value, rel=1e-6, abs=1e-6)
        if (
            isinstance(tsv_value, list)
            and tsv_value
            and isinstance(tsv_value[0], float)
        ):
            return all(
                b == pytest.approx(t, rel=1e-6, abs=1e-6)
                for t, b in zip(tsv_value, bin_value)
            )
        return tsv_value == bin_value

    async def test_all_types_match_tsv(self):
        binary = self._binary_client()
        tsv_rows = await self.ch.fetch("SELECT * FROM all_types ORDER BY uint8")
        bin_rows = await binary.fetch("SELECT * FROM all_types ORDER BY uint8")
        assert len(tsv_rows) == len(bin_rows)
        for tsv_row, bin_row in zip(tsv_rows, bin_rows):
            for key in tsv_row.keys():
                assert self._eq(tsv_row[key], bin_row[key]), key

    async def test_fetchval_and_iterate(self):
        binary = self._binary_client()
        assert await binary.fetchval("SELECT 123::UInt32") == 123
        rows = [row[0] async for row in binary.iterate("SELECT number FROM numbers(3)")]
        assert rows == [0, 1, 2]

    async def test_single_column_empty_string(self):
        # RowBinary length-prefixes strings, so an empty single-column value is
        # unambiguous (unlike the TSV path, issue #14).
        binary = self._binary_client()
        assert (await binary.fetchrow("SELECT '' AS s"))["s"] == ""

    async def test_insert_round_trip(self):
        binary = self._binary_client()
        await binary.execute("DROP TABLE IF EXISTS rb_insert")
        await binary.execute("""
            CREATE TABLE rb_insert (
                u8 UInt8, i64 Int64, f Float64, s String, fs FixedString(4),
                d Date, dttm DateTime('UTC'),
                dt64 DateTime64(3, 'Europe/Moscow'),
                dec Decimal(18, 4), uu UUID, ip4 IPv4, ip6 IPv6,
                e Enum8('a' = 1, 'b' = 2), arr Array(UInt8),
                m Map(String, UInt8), nn Nullable(UInt8), tup Tuple(UInt8, String)
            ) ENGINE = Memory
            """)
        row = (
            7,
            -5,
            3.5,
            "hi",
            "abcd",
            dt.date(2021, 5, 6),
            dt.datetime(2021, 5, 6, 7, 8, 9),
            dt.datetime(2021, 5, 6, 10, 8, 9, 123000),
            Decimal("12.3456"),
            UUID("1ea47c97-16a8-4338-877e-66f464374944"),
            IPv4Address("1.2.3.4"),
            IPv6Address("::1"),
            "b",
            [1, 2, 3],
            {"k": 9},
            None,
            (4, "z"),
        )
        await binary.execute("INSERT INTO rb_insert VALUES", row)
        # Read back with both engines: the binary insert must match exactly.
        assert (await binary.fetchrow("SELECT * FROM rb_insert"))[:] == row
        assert (await self.ch.fetchrow("SELECT * FROM rb_insert"))[:] == row
        await binary.execute("DROP TABLE IF EXISTS rb_insert")

    async def test_insert_with_column_list(self):
        binary = self._binary_client()
        await binary.execute("DROP TABLE IF EXISTS rb_insert_cols")
        await binary.execute(
            "CREATE TABLE rb_insert_cols (a UInt8, b String, c UInt8) ENGINE = Memory"
        )
        # Columns out of table order — types must be looked up in INSERT order.
        await binary.execute("INSERT INTO rb_insert_cols (c, a) VALUES", (3, 1))
        assert (await binary.fetchrow("SELECT a, b, c FROM rb_insert_cols"))[:] == (
            1,
            "",
            3,
        )
        await binary.execute("DROP TABLE IF EXISTS rb_insert_cols")


@pytest.mark.usefixtures("class_chclient")
class TestNative:
    # https://github.com/maximdanilchenko/aiochclient/issues/134
    # The columnar Native read engine. Phase 1: numerics, String, FixedString,
    # Date, DateTime (UTC), Bool, Nullable, Array. Phase 2 adds the scalar
    # long-tail (Decimal, DateTime64, Enum, UUID, IPv4/6, Int128/256, ...) plus
    # columnar Tuple and Map. Phase 3 adds LowCardinality and Nested (covered by
    # the cross-engine TestTypes matrix).
    NATIVE_DDL = """
        CREATE TABLE native_t (
            id UInt32, big Int64, neg Int16, score Float64, f32 Float32,
            name String, fixed FixedString(4), created Date, ts DateTime,
            flag Bool, opt Nullable(Int32), arr Array(UInt32), sarr Array(String)
        ) ENGINE = Memory
        """

    def _native_client(self):
        return ChClient(self.ch._http_client._session, native=True)

    async def test_decode_matches_tsv(self):
        native = self._native_client()
        await native.execute("DROP TABLE IF EXISTS native_t")
        await native.execute(self.NATIVE_DDL)
        rows = [
            (
                i,
                i * 1_000_000_000,
                -i,
                i + 0.5,
                1.5,
                f"name {i}",
                "abcd",
                dt.date(2021, 6, 1),
                dt.datetime(2021, 6, 1, 12, 30, 0),
                bool(i % 2),
                (i if i % 2 else None),
                [1, 2, 3],
                ["alpha", "beta"],
            )
            for i in range(5)
        ]
        await native.execute("INSERT INTO native_t VALUES", *rows)
        tsv_rows = [
            r[:] for r in await self.ch.fetch("SELECT * FROM native_t ORDER BY id")
        ]
        bin_rows = [
            r[:] for r in await native.fetch("SELECT * FROM native_t ORDER BY id")
        ]
        assert tsv_rows == bin_rows
        await native.execute("DROP TABLE IF EXISTS native_t")

    async def test_fetchval_iterate_and_mapping(self):
        native = self._native_client()
        assert await native.fetchval("SELECT 7::UInt32") == 7
        record = await native.fetchrow("SELECT 1 AS a, 'x' AS b")
        assert record["a"] == 1 and record["b"] == "x" and record[0] == 1
        rows = [r[0] async for r in native.iterate("SELECT number FROM numbers(3)")]
        assert rows == [0, 1, 2]

    EXTENDED_DDL = """
        CREATE TABLE native_ext (
            id UInt32, dec Decimal(18, 4), ts64 DateTime64(3),
            en Enum8('a' = 1, 'b' = 2), uid UUID, ip4 IPv4, ip6 IPv6,
            big Int128, tup Tuple(UInt8, String), ntup Tuple(x UInt8, y String),
            mp Map(String, UInt32), arr_tup Array(Tuple(UInt8, String)),
            nl Nullable(Decimal(10, 2))
        ) ENGINE = Memory
        """

    async def test_extended_types_match_tsv(self):
        native = self._native_client()
        await native.execute("DROP TABLE IF EXISTS native_ext")
        await native.execute(self.EXTENDED_DDL)
        rows = [
            (
                i,
                Decimal("3.1416"),
                dt.datetime(2021, 6, 1, 12, 30, 0, 123000),
                "b",
                UUID("12345678-1234-5678-1234-567812345678"),
                IPv4Address("1.2.3.4"),
                IPv6Address("::1"),
                170141183460469231731687303715884105727,
                (7, "hi"),
                (9, "yo"),
                {"k1": 10, "k2": 20},
                [(1, "a"), (2, "b")],
                (Decimal("5.50") if i % 2 else None),
            )
            for i in range(4)
        ]
        await native.execute("INSERT INTO native_ext VALUES", *rows)
        tsv_rows = [
            r[:] for r in await self.ch.fetch("SELECT * FROM native_ext ORDER BY id")
        ]
        nat_rows = [
            r[:] for r in await native.fetch("SELECT * FROM native_ext ORDER BY id")
        ]
        assert tsv_rows == nat_rows
        await native.execute("DROP TABLE IF EXISTS native_ext")

    async def test_unsupported_type_raises(self):
        native = self._native_client()
        # Geo types (here Point) are not in the type mapping, so decoding must
        # raise a clear error rather than silently misread the column.
        with pytest.raises(ChClientError):
            await native.fetchval("SELECT (1.0, 2.0)::Point")


class TestErrorBody:
    # https://github.com/maximdanilchenko/aiochclient/issues/126
    # A non-200 response with an empty body must still raise a ChClientError
    # carrying a non-empty message.
    async def test_aiohttp_empty_error_body(self):
        from aiochclient.http_clients import aiohttp as aiohttp_client

        class _FakeResp:
            status = 500

            async def read(self):
                return b""

        with pytest.raises(ChClientError) as exc:
            await aiohttp_client._check_response(_FakeResp())
        assert str(exc.value).strip()
        assert "500" in str(exc.value)

    async def test_httpx_empty_error_body(self):
        from aiochclient.http_clients import httpx as httpx_client

        class _FakeResp:
            status_code = 502

            async def aread(self):
                return b"   "

        with pytest.raises(ChClientError) as exc:
            await httpx_client._check_response(_FakeResp())
        assert str(exc.value).strip()
        assert "502" in str(exc.value)


class TestExceptionTrailer:
    # https://github.com/maximdanilchenko/aiochclient/issues/93
    # ClickHouse commits a 200 status before streaming a large result, so a
    # mid-stream error is reported via the X-ClickHouse-Exception-Code trailer
    # and an __exception__ envelope appended to the body, not the status code.
    OK_BODY = b"a\nString\nx\ny\n"
    ERR_BODY = (
        b"a\nString\nx\n"
        b"__exception__\ntag\n"
        b"Code: 159. DB::Exception: Timeout exceeded. (TIMEOUT_EXCEEDED)\n"
        b"__exception__\n"
    )
    ERR_HEADERS = {"X-ClickHouse-Exception-Code": "159"}

    async def test_raise_if_exception_code_helper(self):
        from aiochclient.http_clients import abc

        # no code -> no error
        abc.raise_if_exception_code(None, [])
        # code present -> ChClientError carrying the server message
        with pytest.raises(ChClientError) as exc:
            abc.raise_if_exception_code(
                "159",
                [
                    b"__exception__",
                    b"Code: 159. DB::Exception: Timeout. (TIMEOUT_EXCEEDED)",
                ],
            )
        assert "TIMEOUT_EXCEEDED" in str(exc.value)
        # code present but no envelope text -> falls back to the code
        with pytest.raises(ChClientError) as exc:
            abc.raise_if_exception_code("241", [])
        assert "241" in str(exc.value)

    async def _collect(self, client):
        return [line async for line in client.post_return_lines("url", {}, {}, b"")]

    async def test_aiohttp_trailer_stops_stream(self):
        from aiochclient.http_clients.aiohttp import AiohttpHttpClient

        # success: all data lines yielded, no error
        client = AiohttpHttpClient(_FakeAioSession(200, {}, self.OK_BODY))
        assert await self._collect(client) == [b"a\n", b"String\n", b"x\n", b"y\n"]

        # mid-stream exception: data yielded, envelope skipped, error raised
        client = AiohttpHttpClient(
            _FakeAioSession(200, self.ERR_HEADERS, self.ERR_BODY)
        )
        with pytest.raises(ChClientError) as exc:
            await self._collect(client)
        assert "TIMEOUT_EXCEEDED" in str(exc.value)

    async def test_httpx_trailer_stops_stream(self):
        from aiochclient.http_clients.httpx import HttpxHttpClient

        client = HttpxHttpClient(_FakeHttpxSession(200, {}, self.OK_BODY))
        assert await self._collect(client) == [b"a\n", b"String\n", b"x\n", b"y\n"]

        client = HttpxHttpClient(
            _FakeHttpxSession(200, self.ERR_HEADERS, self.ERR_BODY)
        )
        with pytest.raises(ChClientError) as exc:
            await self._collect(client)
        assert "TIMEOUT_EXCEEDED" in str(exc.value)


class _FakeAioResp:
    def __init__(self, status, headers, body):
        self.status = status
        self.headers = headers
        self._body = body

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    class _Content:
        def __init__(self, body):
            self._body = body

        async def iter_any(self):
            yield self._body

    @property
    def content(self):
        return self._Content(self._body)

    async def read(self):
        return self._body


class _FakeAioSession:
    def __init__(self, status, headers, body):
        self._resp = _FakeAioResp(status, headers, body)

    def post(self, **kwargs):
        return self._resp


class _FakeHttpxResp:
    def __init__(self, status, headers, body):
        self.status_code = status
        self.headers = headers
        self._body = body

    async def aiter_bytes(self):
        yield self._body

    async def aread(self):
        return self._body


class _FakeHttpxSession:
    def __init__(self, status, headers, body):
        self._resp = _FakeHttpxResp(status, headers, body)

    async def post(self, **kwargs):
        return self._resp
