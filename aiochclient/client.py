from typing import Any, AsyncGenerator
from aiohttp import client
from aiochclient.records import RecordsFabric
from aiochclient.exceptions import ChClientError


class ChClient:

    __slots__ = ("_session", "url", "params")

    def __init__(
        self,
        session: client.ClientSession,
        *,
        url: str = "http://localhost:8123/",
        user: str = None,
        password: str = None,
        database: str = "default",
        compress_response: bool = False,
    ):
        """
        ChClient connection class.

        Usage:

        .. code-block:: python

            async with aiohttp.ClientSession() as s:
                client = ChClient(s, compress_response=True)
                assert await client.fetch("SELECT number FROM system.numbers LIMIT 100")

        :param aiohttp.ClientSession session:
            aiohttp client session. Please, use one session
            and one ChClient for all connections in your app.

        :param str url:
            Clickhouse server url. Need full path, like "http://localhost:8123/".

        :param str user:
            User name for authorization.

        :param str password:
            Password for authorization.

        :param str database:
            Database name.

        :param bool compress_response:
            Pass True if you want Clickhouse to compress its responses with gzip.
            They will be decompressed automatically. But overall it will be slightly slower.
        """
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
        """
        Checks if connection is Ok.

        Usage:

        .. code-block:: python

            assert await client.is_alive()

        :return: True if connection Ok. False instead.
        """
        async with self._session.get(
            url=self.url
        ) as resp:  # type: client.ClientResponse
            return resp.status == 200

    async def execute(self, query: str) -> None:
        """
        Execute query without reading the response.

        :param query: Clickhouse query string.

        Usage:

        .. code-block:: python

            await client.execute("CREATE TABLE t (a UInt8, b Tuple(Date, Nullable(Float32))) ENGINE = Memory")
            await client.execute("INSERT INTO t VALUES (1, ('2018-09-07', NULL)),(2, ('2018-09-08', 3.14))")

        :return: Nothing.
        """
        async with self._session.post(
            self.url, params=self.params, data=query.encode()
        ) as resp:
            if resp.status != 200:
                raise ChClientError((await resp.read()).decode())

    async def fetch(self, query: str) -> list:
        """
        Execute query and fetch all rows from query result at once.

        :param query: Clickhouse query string (One of SELECT, SHOW, DESCRIBE or EXISTS).

        Usage:

        .. code-block:: python

            all_rows = await client.fetch("SELECT * FROM t")

        :return: All rows from query.
        """
        return [row async for row in self.cursor(query)]

    async def fetchone(self, query: str) -> tuple or None:
        """
        Execute query and fetch first row from query result.

        :param query: Clickhouse query string (One of SELECT, SHOW, DESCRIBE or EXISTS).

        Usage:

        .. code-block:: python

            row = await client.fetchone("SELECT * FROM t WHERE a=1")
            assert row == (1, (dt.date(2018, 9, 7), None))

        :return: First row from query or None if there no results.
        """
        async for row in self.cursor(query):
            return row
        return None

    async def fetchval(self, query: str) -> Any:
        """
        Execute query and fetch first value of the first row from query result.

        :param query: Clickhouse query string (One of SELECT, SHOW, DESCRIBE or EXISTS).

        Usage:

        .. code-block:: python

            assert await client.fetchval("EXISTS TABLE t")

        :return: First value of the first row or None if there no results.
        """
        async for row in self.cursor(query):
            return row[0]
        return None

    async def cursor(self, query: str) -> AsyncGenerator[tuple, None]:
        """
        Async generator by all rows from query result.

        :param query: Clickhouse query string (One of SELECT, SHOW, DESCRIBE or EXISTS).

        Usage:

        .. code-block:: python

            async for row in client.cursor(
                "SELECT number, number*2 FROM system.numbers LIMIT 10000"
            ):
                assert row[0] * 2 == row[1]

        :return: Rows one by one.
        """
        check = query.lstrip()[:8].upper()
        if not any(
                [
                    check.startswith("SELECT"),
                    check.startswith("SHOW"),
                    check.startswith("DESCRIBE"),
                    check.startswith("EXISTS"),
                ]
        ):
            raise ChClientError(
                "Query for fetching should starts with 'SELECT', 'SHOW', 'DESCRIBE' or 'EXISTS'"
            )
        query += " FORMAT TSVWithNamesAndTypes"
        async with self._session.post(
                self.url, params=self.params, data=query.encode()
        ) as resp:  # type: client.ClientResponse
            if resp.status != 200:
                raise ChClientError((await resp.read()).decode())
            await resp.content.readline()
            rf = RecordsFabric(await resp.content.readline())
            async for line in resp.content:
                yield rf.new(line)
