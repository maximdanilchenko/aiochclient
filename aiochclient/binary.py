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

from aiochclient.exceptions import NeedMoreData
from aiochclient.records import Record

# The type objects carry the read/write logic (pure Python). The hot per-value
# buffer access goes through the compiled Cursor when the Cython extension is
# available, falling back to the pure-Python Cursor otherwise.
from aiochclient.types import what_py_type

try:
    from aiochclient._types import Cursor
except ImportError:
    from aiochclient.types import Cursor


def read_binary_str(cursor) -> bytes:
    """Read a length-prefixed (LEB128) byte string."""
    length = cursor.read_varint()
    return cursor.read(length)


class BinaryReader:
    """Buffers an async stream of chunks and runs synchronous parsers over it.

    A committed position advances through the buffer as items are parsed (no
    per-item slicing — that would be quadratic on a large result). The consumed
    prefix is dropped only when more data has to be fetched, which keeps memory
    bounded for streaming without copying on the hot path.
    """

    __slots__ = ("_source", "_buf", "_pos", "_exhausted")

    def __init__(self, source: AsyncGenerator[bytes, None]):
        self._source = source.__aiter__()
        self._buf = b""
        self._pos = 0
        self._exhausted = False

    async def _more(self) -> bool:
        if self._exhausted:
            return False
        try:
            chunk = await self._source.__anext__()
        except StopAsyncIteration:
            self._exhausted = True
            return False
        # Drop the already-consumed prefix before growing the buffer.
        if self._pos:
            self._buf = self._buf[self._pos :]
            self._pos = 0
        self._buf += chunk
        return True

    async def parse(self, fn: Callable[[Cursor], Any]) -> Any:
        """Run ``fn(cursor)``, refilling the buffer on ``NeedMoreData``."""
        while True:
            cursor = Cursor(self._buf, self._pos)
            try:
                result = fn(cursor)
            except NeedMoreData:
                if not await self._more():
                    raise
                continue
            self._pos = cursor.pos
            return result


async def _read_header(reader: BinaryReader):
    """Read a RowBinaryWithNamesAndTypes header -> (names, type strings)."""
    num_columns = await reader.parse(lambda cursor: cursor.read_varint())
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
