from abc import ABC, abstractmethod
from typing import Any, AsyncGenerator, List, Optional

from aiochclient.exceptions import ChClientError

# ClickHouse commits an HTTP 200 once it starts streaming a large result, so an
# error that happens afterwards cannot change the status code. It is instead
# reported through the ``X-ClickHouse-Exception-Code`` trailing header and, on
# recent servers, an ``__exception__`` envelope appended to the response body.
# See https://github.com/maximdanilchenko/aiochclient/issues/93
EXCEPTION_CODE_HEADER = 'X-ClickHouse-Exception-Code'
EXCEPTION_MARKER = b'__exception__'


def raise_if_exception_code(
    exception_code: Optional[str], exception_lines: List[bytes]
) -> None:
    """Raise ``ChClientError`` if ClickHouse reported a mid-stream exception.

    ``exception_code`` is the value of the ``X-ClickHouse-Exception-Code``
    response/trailer header (``None`` when the query succeeded).
    ``exception_lines`` are the body lines collected from the ``__exception__``
    envelope, used to build a helpful message when the server provides it.
    """
    if exception_code is None:
        return
    text = ' '.join(
        line.decode(errors='replace').strip()
        for line in exception_lines
        if line.startswith(b'Code:') or b'Exception' in line
    ).strip()
    raise ChClientError(text or f"ClickHouse returned exception code {exception_code}")


class HttpClientABC(ABC):
    @abstractmethod
    async def get(self, url: str, params: dict, headers: dict) -> None:
        """Use aiochclient.exceptions.ChClientError in case of bad status code"""

    @abstractmethod
    async def post_return_lines(
        self, url: str, params: dict, headers: dict, data: Any
    ) -> AsyncGenerator[bytes, None]:
        """Use aiochclient.exceptions.ChClientError in case of bad status code"""

    @abstractmethod
    async def post_no_return(
        self, url: str, params: dict, headers: dict, data: Any
    ) -> None:
        """Use aiochclient.exceptions.ChClientError in case of bad status code"""

    @abstractmethod
    async def close(self) -> None:
        """Close http session"""

    @staticmethod
    def choose_http_client(session):
        try:
            import aiohttp

            if session is None or isinstance(session, aiohttp.ClientSession):
                from aiochclient.http_clients.aiohttp import AiohttpHttpClient

                return AiohttpHttpClient
        except ImportError:
            pass
        try:
            import httpx

            if session is None or isinstance(session, httpx.AsyncClient):
                from aiochclient.http_clients.httpx import HttpxHttpClient

                return HttpxHttpClient
        except ImportError:
            pass
        raise ChClientError('Async http client heeded. Please install aiohttp or httpx')
