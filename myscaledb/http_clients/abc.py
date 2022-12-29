from abc import ABC, abstractmethod
from typing import Any, AsyncGenerator
# import aiohttp,httpx
from myscaledb.common.exceptions import ClientError


class HttpClientABC(ABC):
    @abstractmethod
    async def get(self, url: str, params: dict) -> None:
        """Use myscaledb.exceptions.ChClientError in case of bad status code"""

    @abstractmethod
    async def post_return_lines(
        self, url: str, params: dict, data: Any
    ) -> AsyncGenerator[bytes, None]:
        """Use myscaledb.exceptions.ChClientError in case of bad status code"""

    @abstractmethod
    async def post_no_return(self, url: str, params: dict, data: Any) -> None:
        """Use myscaledb.exceptions.ChClientError in case of bad status code"""

    @abstractmethod
    async def close(self) -> None:
        """Close http session"""

    @staticmethod
    def choose_http_client(session):
        try:
            import aiohttp

            if session is None or isinstance(session, aiohttp.ClientSession):
                from myscaledb.http_clients.aiohttp import AiohttpHttpClient

                return AiohttpHttpClient
        except ImportError:
            pass
        try:
            import httpx

            if session is None or isinstance(session, httpx.AsyncClient):
                from myscaledb.http_clients.httpx import HttpxHttpClient

                return HttpxHttpClient
        except ImportError:
            pass
        raise ClientError('Async http client heeded. Please install aiohttp or httpx')
