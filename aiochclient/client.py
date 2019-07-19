import warnings
from enum import Enum
from typing import Any, AsyncGenerator, List, Optional

from aiohttp import client

from aiochclient.exceptions import ChClientError
from aiochclient.records import Record, RecordsFabric

# Optional cython extension:
try:
    from aiochclient._types import rows2ch
except ImportError:
    from aiochclient.types import rows2ch


class QueryTypes(Enum):
    FETCH = 0
    INSERT = 1
    OTHER = 2


class ChClient:
    """ChClient connection class.

    Usage:

    .. code-block:: python

        async with aiohttp.ClientSession() as s:
            client = ChClient(s, compress_response=True)
            nums = await client.fetch("SELECT number FROM system.numbers LIMIT 100")

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

    :param **settings:
        Any settings from https://clickhouse.yandex/docs/en/operations/settings
    """

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
        **settings,
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
        self.params.update(settings)

    @classmethod
    def query_type(cls, query: str) -> QueryTypes:
        check = query.lstrip()[:8].upper()
        if any(
            [
                check.startswith("SELECT"),
                check.startswith("SHOW"),
                check.startswith("DESCRIBE"),
                check.startswith("EXISTS"),
            ]
        ):
            return QueryTypes.FETCH
        if check.startswith("INSERT"):
            return QueryTypes.INSERT
        return QueryTypes.OTHER

    async def is_alive(self) -> bool:
        """Checks if connection is Ok.

        Usage:

        .. code-block:: python

            assert await client.is_alive()

        :return: True if connection Ok. False instead.
        """
        async with self._session.get(
            url=self.url
        ) as resp:  # type: client.ClientResponse
            return resp.status == 200

    async def _execute(self, query: str, *args) -> AsyncGenerator[Record, None]:
        query_type = self.query_type(query)

        if query_type == QueryTypes.FETCH:
            query += " FORMAT TSVWithNamesAndTypes"
        if args:
            if query_type != QueryTypes.INSERT:
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
                raise ChClientError((await resp.read()).decode(errors='replace'))
            if query_type == QueryTypes.FETCH:
                rf = RecordsFabric(
                    names=await resp.content.readline(),
                    tps=await resp.content.readline(),
                )
                async for line in resp.content:
                    yield rf.new(line)

    async def execute(self, query: str, *args) -> None:
        """Execute query. Returns None.

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

    async def fetch(self, query: str, *args) -> List[Record]:
        """Execute query and fetch all rows from query result at once in a list.

        :param query: Clickhouse query string.

        Usage:

        .. code-block:: python

            all_rows = await client.fetch("SELECT * FROM t")

        :return: All rows from query.
        """
        return [row async for row in self._execute(query, *args)]

    async def fetchrow(self, query: str, *args) -> Optional[Record]:
        """Execute query and fetch first row from query result or None.

        :param query: Clickhouse query string.

        Usage:

        .. code-block:: python

            row = await client.fetchrow("SELECT * FROM t WHERE a=1")
            assert row[0] == 1
            assert row["b"] == (dt.date(2018, 9, 7), None)

        :return: First row from query or None if there no results.
        """
        async for row in self._execute(query, *args):
            return row
        return None

    async def fetchone(self, query: str, *args) -> Optional[Record]:
        """Deprecated. Use ``fetchrow`` method instead"""
        warnings.warn(
            "'fetchone' method is deprecated. Use 'fetchrow' method instead",
            PendingDeprecationWarning,
        )
        return await self.fetchrow(query, *args)

    async def fetchval(self, query: str, *args) -> Any:
        """Execute query and fetch first value of the first row from query result or None.

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

    async def iterate(self, query: str, *args) -> AsyncGenerator[Record, None]:
        """Async generator by all rows from query result.

        :param str query: Clickhouse query string.

        Usage:

        .. code-block:: python

            async for row in client.iterate(
                "SELECT number, number*2 FROM system.numbers LIMIT 10000"
            ):
                assert row[0] * 2 == row[1]

        :return: Rows one by one.
        """
        async for row in self._execute(query, *args):
            yield row

    async def cursor(self, query: str, *args) -> AsyncGenerator[Record, None]:
        """Deprecated. Use ``iterate`` method instead"""
        warnings.warn(
            "'cursor' method is deprecated. Use 'iterate' method instead",
            PendingDeprecationWarning,
        )
        async for row in self.iterate(query, *args):
            yield row
