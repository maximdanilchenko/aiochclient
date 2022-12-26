from typing import Any, AsyncGenerator, Optional

from httpx import AsyncClient, Response

from myscaledb.common.exceptions import ClientError
from myscaledb.http_clients.abc import HttpClientABC


class HttpxHttpClient(HttpClientABC):
    def __init__(self, session: Optional[AsyncClient]):
        if session:
            self._session = session
        else:
            self._session = AsyncClient()

    async def get(self, url: str, params: dict) -> None:
        resp = await self._session.get(url=url, params=params)
        await _check_response(resp)

    async def post_return_lines(
        self, url: str, params: dict, data: Any
    ) -> AsyncGenerator[bytes, None]:
        resp = await self._session.post(url=url, params=params, content=data)
        await _check_response(resp)
        async for line in resp.aiter_lines():
            yield line.encode()

    async def post_no_return(self, url: str, params: dict, data: Any) -> None:
        resp = await self._session.post(url=url, params=params, content=data)
        await _check_response(resp)

    async def close(self) -> None:
        await self._session.aclose()


async def _check_response(resp):
    if resp.status_code != 200:
        raise ClientError(await _read_error_body(resp))


async def _read_error_body(resp: Response):
    return (await resp.aread()).decode(errors='replace')
