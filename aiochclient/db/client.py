import asyncio
import functools
import logging
from types import TracebackType
from typing import Any, Optional, Type, Dict, List, Iterator

from aiochclient.async_db.client import BaseClient as AsyncChClient
from aiochclient.common.exceptions import ChClientError
from aiochclient.common.records import Record


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
            logging.warning("create new event loop...")
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
        raise ChClientError(msg)

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*_args, **_kwargs):
            func_wrapped = func(*_args, **_kwargs)
            if asyncio.iscoroutine(func_wrapped):
                try:
                    loop = asyncio.get_event_loop()
                except RuntimeError:
                    logging.warning("create new event loop...")
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                loop.set_exception_handler(handle_exception)
                logging.info(loop)
                return loop.run_until_complete(func_wrapped)
            return func_wrapped

        return wrapper

    return decorator


class SyncClient(AsyncChClient):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)

    # Context manager support
    def __enter__(self) -> 'SyncClient':
        if not self.is_alive():
            raise ChClientError("ChClient is already closed.")
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
        for row in iterate_async_to_sync(self._execute(
                query,
                *args,
                json=json,
                query_params=params,
                query_id=query_id,
                decode=decode,
        ), asyncio.get_event_loop()):
            yield row

