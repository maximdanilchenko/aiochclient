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

from aiochclient.binary import (
    BinaryReader,
    read_binary_str,
    read_column,
    what_py_type,
)
from aiochclient.exceptions import ChClientError, NeedMoreData
from aiochclient.records import Record, record_from_decoded

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


# LowCardinality index width: the flags word's low byte selects the key type.
_LC_INDEX_TYPE = {0: "UInt8", 1: "UInt16", 2: "UInt32", 3: "UInt64"}


def _split_args(spec):
    """Split a comma-separated type-argument list at the top level.

    Commas inside nested parentheses or single-quoted literals (e.g. an
    ``Enum8('a'=1,'b'=2)`` element) are not split points.
    """
    parts = []
    depth = 0
    in_quote = False
    start = 0
    i = 0
    length = len(spec)
    while i < length:
        ch = spec[i]
        if in_quote:
            if ch == "\\":
                i += 2
                continue
            if ch == "'":
                in_quote = False
        elif ch == "'":
            in_quote = True
        elif ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        elif ch == "," and depth == 0:
            parts.append(spec[start:i].strip())
            start = i + 1
        i += 1
    parts.append(spec[start:].strip())
    return parts


def _element_type(spec):
    """Strip an optional leading ``name `` from a (possibly named) Tuple element."""
    depth = 0
    in_quote = False
    for i, ch in enumerate(spec):
        if in_quote:
            if ch == "'":
                in_quote = False
        elif ch == "'":
            in_quote = True
        elif ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        elif ch == " " and depth == 0:
            return spec[i + 1 :].strip()
    return spec


def _read_prefix(cursor, ctype):
    """Consume the column's serialization-state prefix.

    Before the column body, ClickHouse writes a UInt64 key-serialization
    version for every LowCardinality found anywhere in the type tree
    (depth-first). For a top-level LowCardinality this byte run sits right in
    front of its body, but inside an Array/Map/Tuple it is hoisted ahead of the
    offsets — so the prefix must be read as a separate pass over the type tree.
    """
    if ctype.startswith("LowCardinality("):
        cursor.read(8)  # KeysSerializationVersion (always 1)
        _read_prefix(cursor, ctype[15:-1])
    elif ctype.startswith("Array("):
        _read_prefix(cursor, ctype[6:-1])
    elif ctype.startswith("Nullable("):
        _read_prefix(cursor, ctype[9:-1])
    elif ctype.startswith("Nested("):
        _read_prefix(cursor, "Tuple(" + ctype[7:-1] + ")")
    elif ctype.startswith("Tuple("):
        for spec in _split_args(ctype[6:-1]):
            _read_prefix(cursor, _element_type(spec))
    elif ctype.startswith("Map("):
        ktype, vtype = _split_args(ctype[4:-1])
        _read_prefix(cursor, ktype)
        _read_prefix(cursor, vtype)


def decode_column_with_prefix(cursor, n, ctype):
    """Read a whole top-level column: its state prefix, then its body."""
    _read_prefix(cursor, ctype)
    return decode_column(cursor, n, ctype)


def decode_column(cursor, n, ctype):
    """Decode the body of one column of ``n`` values from the cursor."""
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
    if ctype.startswith("Tuple("):
        # Native stores a Tuple column as one sub-column per element.
        specs = _split_args(ctype[6:-1])
        subcolumns = [decode_column(cursor, n, _element_type(s)) for s in specs]
        if not subcolumns:
            return [() for _ in range(n)]
        return list(zip(*subcolumns))
    if ctype.startswith("Map("):
        # Like Array(Tuple(K, V)): offsets, then a key and a value sub-column.
        ktype, vtype = _split_args(ctype[4:-1])
        offsets = decode_column(cursor, n, "UInt64")
        total = offsets[-1] if n else 0
        keys = decode_column(cursor, total, ktype)
        values = decode_column(cursor, total, vtype)
        out = []
        prev = 0
        for offset in offsets:
            out.append(dict(zip(keys[prev:offset], values[prev:offset])))
            prev = offset
        return out
    if ctype.startswith("LowCardinality("):
        # Per block, a LowCardinality column is: a UInt64 key-serialization
        # version, a UInt64 flags word (its low byte selects the index width),
        # the dictionary (a UInt64 size then that many inner values), and finally
        # a UInt64 index count and that many indexes into the dictionary.
        inner = ctype[15:-1]
        nullable = inner.startswith("Nullable(")
        dict_type = inner[9:-1] if nullable else inner
        # The UInt64 key-serialization version was already consumed by the
        # column's prefix pass (see _read_prefix); the body starts at the flags.
        flags = int.from_bytes(cursor.read(8), "little")
        index_type = _LC_INDEX_TYPE[flags & 0xFF]
        dict_size = int.from_bytes(cursor.read(8), "little")
        dictionary = decode_column(cursor, dict_size, dict_type)
        num_keys = int.from_bytes(cursor.read(8), "little")
        indexes = decode_column(cursor, num_keys, index_type)
        if nullable:
            # Index 0 is reserved for NULL (its dictionary slot is a placeholder).
            return [None if i == 0 else dictionary[i] for i in indexes]
        return [dictionary[i] for i in indexes]
    if ctype.startswith("Nested("):
        # With flatten_nested=0 a Nested column is laid out exactly like
        # Array(Tuple(...)) of its sub-fields (shared offsets, then each field
        # as a flat sub-column), so decode it as such.
        return decode_column(cursor, n, "Array(Tuple(" + ctype[7:-1] + "))")
    # Generic per-value fallback through the compiled RowBinary readers — covers
    # Decimal, DateTime64, Enum, UUID, IPv4/6, Int128/256 and any other
    # fixed-layout scalar whose Native column is its RowBinary values back to back.
    return read_column(cursor, what_py_type(ctype), n)


def _read_varint(cursor):
    return cursor.read_varint()


async def blocks_from_native(
    source: AsyncGenerator[bytes, None],
) -> AsyncGenerator[list, None]:
    """Yield one list of :class:`Record` per ``Native`` block.

    Decoding (and building the rows for) a whole block at once, then handing the
    list back in a single ``async`` step, avoids pulling 10000s of rows one by
    one through the async-generator protocol — that per-item cost otherwise
    roughly halves throughput versus the raw columnar decode.
    """
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
                    lambda cursor, ct=ctype, nr=num_rows: decode_column_with_prefix(
                        cursor, nr, ct
                    )
                )
            )
        if not num_rows:
            continue
        name_map = {name: index for index, name in enumerate(names)}
        yield [record_from_decoded(values, name_map) for values in zip(*columns)]


async def rows_from_native(
    source: AsyncGenerator[bytes, None],
) -> AsyncGenerator[Record, None]:
    """Yield :class:`Record` per row of a ``Native`` stream."""
    async for block in blocks_from_native(source):
        for record in block:
            yield record
