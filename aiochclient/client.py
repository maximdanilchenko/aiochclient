from enum import Enum
from typing import Any, AsyncGenerator

from aiohttp import client

from aiochclient.exceptions import ChClientError
from aiochclient.records import RecordsFabric

# Optional cython extension:
try:
    from aiochclient._types import rows2ch
except ImportError:
    from aiochclient.types import rows2ch


class ChClient:
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

    __slots__ = ("_session", "url", "params")

    class QueryTypes(Enum):
        FETCH = 0
        INSERT = 1
        OTHER = 2

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

    @classmethod
    def query_type(cls, query):
        check = query.lstrip()[:8].upper()
        if any(
            [
                check.startswith("SELECT"),
                check.startswith("SHOW"),
                check.startswith("DESCRIBE"),
                check.startswith("EXISTS"),
            ]
        ):
            return cls.QueryTypes.FETCH
        if check.startswith("INSERT"):
            return cls.QueryTypes.INSERT
        return cls.QueryTypes.OTHER

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

    async def _execute(self, query: str, *args) -> AsyncGenerator[tuple, None]:
        query_type = self.query_type(query)

        if query_type == self.QueryTypes.FETCH:
            query += " FORMAT TSVWithNamesAndTypes"
        if args:
            if query_type != self.QueryTypes.INSERT:
                raise ChClientError(
                    "It is possible to pass arguments only for INSERT queries"
                )
            params = {**self.params, "query": query}
            data = rows2ch(*args)
        else:
            params = self.params
            data = query.encode()

        async with self._session.post(
            self.url, params=params, data=data
        ) as resp:  # type: client.ClientResponse
            if resp.status != 200:
                raise ChClientError((await resp.read()).decode())
            if query_type == self.QueryTypes.FETCH:
                await resp.content.readline()
                rf = RecordsFabric(await resp.content.readline())
                async for line in resp.content:
                    yield rf.new(line)

    async def execute(self, query: str, *args) -> list or None:
        """
        Execute query. Returns None.

        :param query: Clickhouse query string.
        :param args: Arguments for insert queries.

        Usage:

        .. code-block:: python

            await client.execute(
                "CREATE TABLE t (a UInt8, b Tuple(Date, Nullable(Float32))) ENGINE = Memory"
            )
            await client.execute(
                "INSERT INTO t VALUES",
                (1, (dt.date(2018, 9, 7), None)),
                (2, (dt.date(2018, 9, 8), 3.14)),
            )

        :return: Nothing.
        """
        async for _ in self._execute(query, *args):
            return None

    async def fetch(self, query: str, *args) -> list:
        """
        Execute query and fetch all rows from query result at once in a list.

        :param query: Clickhouse query string.

        Usage:

        .. code-block:: python

            all_rows = await client.fetch("SELECT * FROM t")

        :return: All rows from query.
        """
        return [row async for row in self._execute(query, *args)]

    async def fetchone(self, query: str, *args) -> tuple or None:
        """
        Execute query and fetch first row from query result or None.

        :param query: Clickhouse query string.

        Usage:

        .. code-block:: python

            row = await client.fetchone("SELECT * FROM t WHERE a=1")
            assert row == (1, (dt.date(2018, 9, 7), None))

        :return: First row from query or None if there no results.
        """
        async for row in self._execute(query, *args):
            return row
        return None

    async def fetchval(self, query: str, *args) -> Any:
        """
        Execute query and fetch first value of the first row from query result or None.

        :param query: Clickhouse query string.

        Usage:

        .. code-block:: python

            val = await client.fetchval("SELECT b FROM t WHERE a=2")
            assert val == (dt.date(2018, 9, 8), 3.14)

        :return: First value of the first row or None if there no results.
        """
        async for row in self._execute(query, *args):
            if row:
                return row[0]
        return None

    async def cursor(self, query: str, *args) -> AsyncGenerator[tuple, None]:
        """
        Async generator by all rows from query result.

        :param query: Clickhouse query string.

        Usage:

        .. code-block:: python

            async for row in client.cursor(
                "SELECT number, number*2 FROM system.numbers LIMIT 10000"
            ):
                assert row[0] * 2 == row[1]

        :return: Rows one by one.
        """
        async for row in self._execute(query, *args):
            yield row
