from typing import List, AsyncGenerator
from aiohttp import client
from aiochclient.records import RecordFabric


class AioChClient:
    def __init__(
        self,
        session: client.ClientSession,
        url: str = "http://localhost:8123/",
        user: str = None,
        password: str = None,
        database: str = "default",
        compress_response: bool = False,
    ):
        self._session = session
        self.url = url
        self.params = {}
        if user:
            self.params["user"] = user
        if password:
            self.params["password"] = password
        if database:
            self.params["database"] = database
        if compress_response:
            self.params["enable_http_compression"] = 1

    async def is_alive(self) -> bool:
        async with self._session.get(
            url=self.url
        ) as resp:  # type: client.ClientResponse
            return resp.status == 200

    async def cursor(self, query: str) -> AsyncGenerator:
        assert query.lstrip().startswith(
            "SELECT"
        ), "Query for fetching should starts with 'SELECT'"
        query += " FORMAT TSVWithNamesAndTypes"
        async with self._session.post(
            self.url, params=self.params, data=query.encode()
        ) as resp:  # type: client.ClientResponse
            rf = RecordFabric(
                names=await resp.content.readline(), tps=await resp.content.readline()
            )
            async for line in resp.content:
                yield rf.new(line)

    async def execute(self, query: str) -> None:
        await self._session.post(self.url, params=self.params, data=query.encode())

    async def fetch(self, query: str) -> List:
        return [row async for row in self.cursor(query)]
