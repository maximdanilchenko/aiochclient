"""Native (columnar) decoding engine.

ClickHouse's ``Native`` format is column-oriented: a stream of blocks, each a
``varint(num_columns), varint(num_rows)`` header followed, per column, by its
name, type and the ``num_rows`` values laid out contiguously. Decoding a whole
column at once — fixed-width numerics via the stdlib ``array`` module, strings
and dates in a compiled Cursor loop — is far cheaper than reading values one by
one, which is what lets this engine rival the columnar C clients without numpy.
"""

import array
import sys
from typing import AsyncGenerator

from aiochclient.binary import BinaryReader, read_binary_str
from aiochclient.exceptions import ChClientError, NeedMoreData
from aiochclient.records import Record

# Use the compiled Cursor (with bulk column reads) when available.
try:
    from aiochclient._types import Cursor  # noqa: F401
except ImportError:
    from aiochclient.types import Cursor  # noqa: F401

_LE = sys.byteorder == "little"

# Fixed-width ClickHouse numeric type -> (array typecode, byte width).
_NUMERIC = {
    "UInt8": ("B", 1),
    "Int8": ("b", 1),
    "UInt16": ("H", 2),
    "Int16": ("h", 2),
    "UInt32": ("I", 4),
    "Int32": ("i", 4),
    "UInt64": ("Q", 8),
    "Int64": ("q", 8),
    "Float32": ("f", 4),
    "Float64": ("d", 8),
}
for _t, (_c, _w) in _NUMERIC.items():
    assert array.array(_c).itemsize == _w, _t  # platform sanity check


def decode_column(cursor, n, ctype):
    """Decode one column of ``n`` values from the cursor."""
    spec = _NUMERIC.get(ctype)
    if spec is not None:
        code, width = spec
        column = array.array(code)
        column.frombytes(cursor.read(n * width))
        if not _LE:
            column.byteswap()
        return column.tolist()
    if ctype == "Bool":
        return [byte != 0 for byte in cursor.read(n)]
    if ctype == "String":
        return cursor.read_string_column(n)
    if ctype.startswith("FixedString"):
        width = int(ctype[12:-1])
        data = cursor.read(n * width)
        return [data[i * width : (i + 1) * width].decode() for i in range(n)]
    if ctype == "Date":
        return cursor.read_date_column(n)
    if ctype == "DateTime" or ctype.startswith("DateTime("):
        # The Native format does not carry the per-column timezone (it always
        # ships the UTC epoch), so DateTime is returned as a naive UTC datetime.
        # The TSV/RowBinary engines apply the column timezone instead.
        return cursor.read_datetime_column(n)
    if ctype.startswith("Nullable("):
        inner = ctype[9:-1]
        flags = cursor.read(n)
        values = decode_column(cursor, n, inner)
        return [None if flags[i] else values[i] for i in range(n)]
    if ctype.startswith("Array("):
        inner = ctype[6:-1]
        offsets = decode_column(cursor, n, "UInt64")
        total = offsets[-1] if n else 0
        flat = decode_column(cursor, total, inner)
        out = []
        prev = 0
        for offset in offsets:
            out.append(flat[prev:offset])
            prev = offset
        return out
    raise ChClientError(f"Native decoding is not implemented for type '{ctype}'")


def _read_varint(cursor):
    return cursor.read_varint()


async def rows_from_native(
    source: AsyncGenerator[bytes, None],
) -> AsyncGenerator[Record, None]:
    """Yield :class:`Record` per row of a ``Native`` stream (block by block)."""
    reader = BinaryReader(source)
    while True:
        try:
            num_columns = await reader.parse(_read_varint)
        except NeedMoreData:
            return  # end of stream
        num_rows = await reader.parse(_read_varint)
        names = []
        columns = []
        # Each column is laid out as: name, type, then its contiguous values.
        for _ in range(num_columns):
            names.append((await reader.parse(read_binary_str)).decode())
            ctype = (await reader.parse(read_binary_str)).decode()
            columns.append(
                await reader.parse(
                    lambda cursor, ct=ctype, nr=num_rows: decode_column(cursor, nr, ct)
                )
            )
        if not num_rows:
            continue
        name_map = {name: index for index, name in enumerate(names)}
        for values in zip(*columns):
            yield Record.from_decoded(values, name_map)
