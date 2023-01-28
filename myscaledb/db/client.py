import asyncio
import functools
import logging
from types import TracebackType
from typing import Any, Optional, Type, Dict, List, Iterator

from myscaledb.async_db.client import BaseClient as AsyncChClient
from myscaledb.common.exceptions import ClientError
from myscaledb.common.records import Record
import nest_asyncio

nest_asyncio.apply()


def iterate_async_to_sync(async_iterate, loop):
    async_iterate = async_iterate.__aiter__()

    async def get_next():
        try:
            obj = await async_iterate.__anext__()
            return False, obj
        except StopAsyncIteration:
            return True, None

    while True:
        try:
            done, obj = loop.run_until_complete(get_next())
        except RuntimeError:
            logging.debug("create new event loop...")
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            done, obj = loop.run_until_complete(get_next())
        if done:
            break
        yield obj


def async_to_sync():
    def handle_exception(loop, context):
        # context["message"] will always be there; but context["exception"] may not
        msg = context.get("exception", context["message"])
        logging.error("Caught exception in async method: %s", msg)
        raise ClientError(msg)

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*_args, **_kwargs):
            func_wrapped = func(*_args, **_kwargs)
            if asyncio.iscoroutine(func_wrapped):
                try:
                    loop = asyncio.get_event_loop()
                except RuntimeError:
                    logging.debug("create new event loop...")
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                loop.set_exception_handler(handle_exception)
                return loop.run_until_complete(func_wrapped)
            return func_wrapped

        return wrapper

    return decorator


class Client(AsyncChClient):
    """Client connection class.

       Usage:

       .. code-block:: python

           from myscaledb import Client

           def main():
               client = Client()
               alive = client.is_alive()
               print(f"Is MyScale alive? -> {alive}")

           if __name__ == '__main__':
               main()

    :param aiohttp.ClientSession session:
        aiohttp client session. Please, use one session
        and one Client for all connections in your app.

    :param str url:
        MyScale server url. Need full path, like http://localhost:8123/.

    :param str user:
        User name for authorization.

    :param str password:
        Password for authorization.

    :param str database:
        Database name.

    :param bool compress_response:
        Pass True if you want MyScale to compress its responses with gzip.
        They will be decompressed automatically. But overall it will be slightly slower.

    :param **settings:
        Any settings from https://clickhouse.yandex/docs/en/operations/settings
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)

    # Context manager support
    def __enter__(self) -> 'Client':
        if not self.is_alive():
            raise ClientError("Client is already closed.")
        return self

    def __exit__(
            self,
            exc_type: Optional[Type[BaseException]],
            exc_val: Optional[BaseException],
            exc_tb: Optional[TracebackType],
    ) -> None:
        self.close()

    def __del__(self):
        self.close()

    @async_to_sync()
    async def close(self) -> None:
        """Close the session"""
        await self._aclose()

    @async_to_sync()
    async def is_alive(self) -> bool:
        return await self._is_alive()

    def execute(
            self,
            query: str,
            *args,
            json: bool = False,
            params: Optional[Dict[str, Any]] = None,
            query_id: str = None,
    ) -> None:
        """Execute query. Returns None.

        :param str query: MyScale query string.
        :param args: Arguments for insert queries.
        :param bool json: Execute query in JSONEachRow mode.
        :param Optional[Dict[str, Any]] params: Params to escape inside query string.
        :param str query_id: MyScale query_id.

        Usage:

        .. code-block:: python

            client.execute(
                "CREATE TABLE t (a UInt8, b Tuple(Date, Nullable(Float32))) ENGINE = Memory"
            )
            client.execute(
                "INSERT INTO t VALUES",
                (1, (dt.date(2018, 9, 7), None)),
                (2, (dt.date(2018, 9, 8), 3.14)),
            )
            client.execute(
                "SELECT * FROM t WHERE a={u8}",
                params={"u8": 12}
            )

        :return: Nothing.
        """
        for _ in iterate_async_to_sync(self._execute(
                query, *args, json=json, query_params=params, query_id=query_id
        ), asyncio.get_event_loop()):
            return None

    def fetch(
            self,
            query: str,
            *args,
            json: bool = False,
            params: Optional[Dict[str, Any]] = None,
            query_id: str = None,
            decode: bool = True,
    ) -> List[Record]:
        """Execute query and fetch all rows from query result at once in a list.

        :param query: MyScale query string.
        :param bool json: Execute query in JSONEachRow mode.
        :param Optional[Dict[str, Any]] params: Params to escape inside query string.
        :param str query_id: MyScale query_id.
        :param decode: Decode to python types. If False, returns bytes for each field instead.

        Usage:

        .. code-block:: python

            all_rows = client.fetch("SELECT * FROM t")

        :return: All rows from query.
        """
        return [
            row
            for row in iterate_async_to_sync(self._execute(
                query,
                *args,
                json=json,
                query_params=params,
                query_id=query_id,
                decode=decode,
            ), asyncio.get_event_loop())
        ]

    def fetchrow(
            self,
            query: str,
            *args,
            json: bool = False,
            params: Optional[Dict[str, Any]] = None,
            query_id: str = None,
            decode: bool = True,
    ) -> Optional[Record]:
        """Execute query and fetch first row from query result or None.

        :param query: MyScale query string.
        :param bool json: Execute query in JSONEachRow mode.
        :param Optional[Dict[str, Any]] params: Params to escape inside query string.
        :param str query_id: MyScale query_id.
        :param decode: Decode to python types. If False, returns bytes for each field instead.

        Usage:

        .. code-block:: python

            row = client.fetchrow("SELECT * FROM t WHERE a=1")
            assert row[0] == 1
            assert row["b"] == (dt.date(2018, 9, 7), None)

        :return: First row from query or None if there no results.
        """
        for row in iterate_async_to_sync(self._execute(
                query,
                *args,
                json=json,
                query_params=params,
                query_id=query_id,
                decode=decode,
        ), asyncio.get_event_loop()):
            return row
        return None

    def fetchval(
            self,
            query: str,
            *args,
            json: bool = False,
            params: Optional[Dict[str, Any]] = None,
            query_id: str = None,
            decode: bool = True,
    ) -> Any:
        """Execute query and fetch first value of the first row from query result or None.

        :param query: MyScale query string.
        :param bool json: Execute query in JSONEachRow mode.
        :param Optional[Dict[str, Any]] params: Params to escape inside query string.
        :param str query_id: MyScale query_id.
        :param decode: Decode to python types. If False, returns bytes for each field instead.

        Usage:

        .. code-block:: python

            val = client.fetchval("SELECT b FROM t WHERE a=2")
            assert val == (dt.date(2018, 9, 8), 3.14)

        :return: First value of the first row or None if there no results.
        """
        for row in iterate_async_to_sync(self._execute(
                query,
                *args,
                json=json,
                query_params=params,
                query_id=query_id,
                decode=decode,
        ), asyncio.get_event_loop()):
            if row:
                return row[0]
        return None

    def iterate(
            self,
            query: str,
            *args,
            json: bool = False,
            params: Optional[Dict[str, Any]] = None,
            query_id: str = None,
            decode: bool = True,
    ) -> Iterator[Record]:
        """Async generator by all rows from query result.

        :param str query: MyScale query string.
        :param bool json: Execute query in JSONEachRow mode.
        :param Optional[Dict[str, Any]] params: Params to escape inside query string.
        :param str query_id: MyScale query_id.
        :param decode: Decode to python types. If False, returns bytes for each field instead.

        Usage:

        .. code-block:: python

            for row in client.iterate(
                "SELECT number, number*2 FROM system.numbers LIMIT 10000"
            ):
                assert row[0] * 2 == row[1]

            for row in client.iterate(
                "SELECT number, number*2 FROM system.numbers LIMIT {numbers_limit}",
                params={"numbers_limit": 10000}
            ):
                assert row[0] * 2 == row[1]

        :return: Rows one by one.
        """
        for row in iterate_async_to_sync(self._execute(
                query,
                *args,
                json=json,
                query_params=params,
                query_id=query_id,
                decode=decode,
        ), asyncio.get_event_loop()):
            yield row
