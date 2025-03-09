import pytest
from aiohttp import ClientSession
from aiochclient import ChClient


@pytest.mark.asyncio
async def test_map_decoding_multiple_pairs():
    async with ClientSession() as session:
        client = ChClient(session, url="http://localhost:8123", user="default", password="")
        result = await client.fetch(
            "SELECT map('a', '1', 'b', '2', 'c', '3') AS data",
            decode=True
        )
        assert len(result) == 1
        assert result[0]['data'] == {'a': '1', 'b': '2', 'c': '3'}, "Multi-pair map decoding failed"


@pytest.mark.asyncio
async def test_map_decoding_single_pair():
    async with ClientSession() as session:
        client = ChClient(session, url="http://localhost:8123")
        result = await client.fetch(
            "SELECT map('x', 'y') AS data",
            decode=True
        )
        assert len(result) == 1
        assert result[0]['data'] == {'x': 'y'}, "Single-pair map decoding failed"


@pytest.mark.asyncio
async def test_map_decoding_empty():
    async with ClientSession() as session:
        client = ChClient(session, url="http://localhost:8123")
        result = await client.fetch(
            "SELECT map() AS data",
            decode=True
        )
        assert len(result) == 1
        assert result[0]['data'] == {}, "Empty map decoding failed"


@pytest.mark.asyncio
async def test_map_table_insert_and_fetch():
    async with ClientSession() as session:
        client = ChClient(session, url="http://localhost:8123")
        await client.execute(
            "CREATE TABLE IF NOT EXISTS test_map (id UInt8, data Map(String, String)) ENGINE = Memory"
        )
        await client.execute(
            "INSERT INTO test_map VALUES",
            (1, {'a': '1', 'b': '2', 'c': '3'})
        )
        result = await client.fetch(
            "SELECT data FROM test_map WHERE id = 1",
            decode=True
        )
        assert len(result) == 1
        assert result[0]['data'] == {'a': '1', 'b': '2', 'c': '3'}, "Table map decoding failed"
        await client.execute("DROP TABLE test_map")