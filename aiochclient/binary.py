"""RowBinary decoding engine.

ClickHouse ``RowBinary``/``RowBinaryWithNamesAndTypes`` packs rows with no
delimiters, so values are read by their exact byte width/length. Parsing is
done with a synchronous :class:`Cursor` over an in-memory buffer; when the
buffer runs short mid-value the cursor raises :class:`NeedMoreData` and the
async driver pulls the next chunk and re-parses from the start of the current
item (items are small, so re-parsing is cheap and keeps the type ``read``
methods synchronous and fast).
"""

from typing import Any, AsyncGenerator, Callable, List

from aiochclient.records import Record

# The RowBinary read path uses the pure-Python type objects, which carry the
# ``read`` methods. (The Cython mirror of ``read`` lands in a later phase.)
from aiochclient.types import what_py_type


class NeedMoreData(Exception):
    """Raised by ``Cursor`` when the buffer does not hold the requested bytes."""


class Cursor:
    """Synchronous forward cursor over a bytes buffer."""

    __slots__ = ("buf", "pos")

    def __init__(self, buf: bytes = b""):
        self.buf = buf
        self.pos = 0

    def read(self, n: int) -> bytes:
        end = self.pos + n
        if end > len(self.buf):
            raise NeedMoreData
        chunk = self.buf[self.pos : end]
        self.pos = end
        return chunk

    def read_varint(self) -> int:
        buf = self.buf
        size = len(buf)
        pos = self.pos
        result = 0
        shift = 0
        while True:
            if pos >= size:
                raise NeedMoreData
            byte = buf[pos]
            pos += 1
            result |= (byte & 0x7F) << shift
            if not byte & 0x80:
                self.pos = pos
                return result
            shift += 7


def read_binary_str(cursor: Cursor) -> bytes:
    """Read a length-prefixed (LEB128) byte string."""
    length = cursor.read_varint()
    return cursor.read(length)


class BinaryReader:
    """Buffers an async stream of chunks and runs synchronous parsers over it."""

    __slots__ = ("_source", "buf", "_exhausted")

    def __init__(self, source: AsyncGenerator[bytes, None]):
        self._source = source.__aiter__()
        self.buf = b""
        self._exhausted = False

    async def _more(self) -> bool:
        if self._exhausted:
            return False
        try:
            chunk = await self._source.__anext__()
        except StopAsyncIteration:
            self._exhausted = True
            return False
        self.buf += chunk
        return True

    async def parse(self, fn: Callable[[Cursor], Any]) -> Any:
        """Run ``fn(cursor)``, refilling the buffer on ``NeedMoreData``."""
        while True:
            cursor = Cursor(self.buf)
            try:
                result = fn(cursor)
            except NeedMoreData:
                if not await self._more():
                    raise
                continue
            self.buf = self.buf[cursor.pos :]
            return result


async def _read_header(reader: BinaryReader):
    """Read a RowBinaryWithNamesAndTypes header -> (names, type strings)."""
    num_columns = await reader.parse(Cursor.read_varint)
    names = [(await reader.parse(read_binary_str)).decode() for _ in range(num_columns)]
    types = [(await reader.parse(read_binary_str)).decode() for _ in range(num_columns)]
    return names, types


async def parse_header(reader: BinaryReader) -> "RowBinaryFabric":
    """Read the RowBinaryWithNamesAndTypes header and build a record fabric."""
    names, types = await _read_header(reader)
    return RowBinaryFabric(names, types)


async def fetch_column_types(source: AsyncGenerator[bytes, None]) -> list:
    """Read just the header of a (LIMIT 0) RowBinaryWithNamesAndTypes response
    and return the column type objects, used to encode INSERT rows."""
    reader = BinaryReader(source)
    _, types = await _read_header(reader)
    return [what_py_type(tp) for tp in types]


def rows_to_binary(rows, types: list) -> bytes:
    """Encode rows (iterables of column values) as a RowBinary body."""
    return b"".join(
        b"".join(tp.write(value) for tp, value in zip(types, row)) for row in rows
    )


class RowBinaryFabric:
    """Builds :class:`Record` objects from RowBinary rows."""

    __slots__ = ("names", "readers", "_read_row")

    def __init__(self, names: List[str], types: List[str], convert: bool = True):
        self.names = {name: index for index, name in enumerate(names)}
        readers = [what_py_type(tp).read for tp in types]
        self.readers = readers

        def _read_row(cursor: Cursor) -> tuple:
            return tuple(read(cursor) for read in readers)

        self._read_row = _read_row

    def read_row(self, cursor: Cursor) -> tuple:
        return self._read_row(cursor)

    def new(self, values: tuple) -> Record:
        return Record.from_decoded(values, self.names)


async def rows_from_binary(
    source: AsyncGenerator[bytes, None],
) -> AsyncGenerator[Record, None]:
    """Yield :class:`Record` per row of a RowBinaryWithNamesAndTypes stream."""
    reader = BinaryReader(source)
    fabric = await parse_header(reader)
    while True:
        try:
            values = await reader.parse(fabric.read_row)
        except NeedMoreData:
            # No complete row left -> end of stream.
            break
        yield fabric.new(values)
