"""Native (columnar) decoding engine.

ClickHouse's ``Native`` format is column-oriented: a stream of blocks, each a
``varint(num_columns), varint(num_rows)`` header followed, per column, by its
name, type and the ``num_rows`` values laid out contiguously. Decoding a whole
column at once — fixed-width numerics via the stdlib ``array`` module, strings
and dates in a compiled Cursor loop — is far cheaper than reading values one by
one, which is what lets this engine rival the columnar C clients without numpy.
"""

import array
import datetime as dt
import gc as _gc
import re
import sys
from decimal import Decimal
from ipaddress import IPv4Address, IPv6Address
from typing import AsyncGenerator
from uuid import UUID

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
    from aiochclient._types import Cursor, to_epoch_micros  # noqa: F401
except ImportError:
    from aiochclient.types import Cursor, to_epoch_micros  # noqa: F401

# Compiled String-column encoder; pure-Python fallback below.
try:
    from aiochclient._types import write_string_column
except ImportError:

    def write_string_column(values):
        out = bytearray()
        for value in values:
            data = value.encode()
            length = len(data)
            while length >= 0x80:
                out.append((length & 0x7F) | 0x80)
                length >>= 7
            out.append(length)
            out += data
        return bytes(out)


# Compiled transpose+Record builder; pure-Python fallback below.
try:
    from aiochclient._types import build_records
except ImportError:

    def build_records(columns, names):
        # Building all rows at once otherwise trips the cyclic GC mid-build; the
        # objects are all about to be returned, so suppress it for this tight,
        # await-free section and restore the collector afterwards.
        gc_was_enabled = _gc.isenabled()
        if gc_was_enabled:
            _gc.disable()
        try:
            return [record_from_decoded(values, names) for values in zip(*columns)]
        finally:
            if gc_was_enabled:
                _gc.enable()


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
    """Read a whole top-level column: its state prefix, then its body.

    Decoding a column allocates its whole value list (and, for strings/objects,
    every element) at once; like the row build, that mass allocation otherwise
    trips the cyclic GC mid-decode. This runs synchronously inside a single
    ``reader.parse`` call (no ``await``), so the collector is disabled for it and
    restored afterwards.
    """
    gc_was_enabled = _gc.isenabled()
    if gc_was_enabled:
        _gc.disable()
    try:
        _read_prefix(cursor, ctype)
        return decode_column(cursor, n, ctype)
    finally:
        if gc_was_enabled:
            _gc.enable()


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
    if ctype == "DateTime":
        # ClickHouse's Native header drops the timezone of a DateTime('TZ')
        # column (it ships plain "DateTime"; DateTime64 keeps its zone), so a
        # timezone DateTime can only come back as naive UTC on this engine.
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
        yield build_records(columns, name_map)


async def rows_from_native(
    source: AsyncGenerator[bytes, None],
) -> AsyncGenerator[Record, None]:
    """Yield :class:`Record` per row of a ``Native`` stream."""
    async for block in blocks_from_native(source):
        for record in block:
            yield record


# --------------------------------------------------------------------------- #
# Native INSERT (columnar write path)
# --------------------------------------------------------------------------- #

# Epochs for the null-slot defaults and the bulk Date/DateTime encoders.
_EPOCH_DATE = dt.date(1970, 1, 1)
_EPOCH_DATETIME = dt.datetime(1970, 1, 1)


def _write_varint(value):
    """Encode an unsigned int as LEB128 (the Native length prefix)."""
    out = bytearray()
    while True:
        byte = value & 0x7F
        value >>= 7
        if value:
            out.append(byte | 0x80)
        else:
            out.append(byte)
            return bytes(out)


def _write_str(value):
    encoded = value.encode()
    return _write_varint(len(encoded)) + encoded


def _wire_type(ctype):
    """Map a column type to the type declared (and encoded) in an INSERT block.

    ClickHouse converts an INSERT block's columns to the table's types, so the
    dictionary-encoded LowCardinality and the Nested sugar do not need to be
    written natively: LowCardinality(T) is sent as T, and Nested(...) as the
    Array(Tuple(...)) it is stored as. Containers are rewritten recursively.
    """
    if ctype.startswith("LowCardinality("):
        return _wire_type(ctype[15:-1])
    if ctype.startswith("Nested("):
        return _wire_type("Array(Tuple(" + ctype[7:-1] + "))")
    if ctype.startswith("Nullable("):
        return "Nullable(" + _wire_type(ctype[9:-1]) + ")"
    if ctype.startswith("Array("):
        return "Array(" + _wire_type(ctype[6:-1]) + ")"
    if ctype.startswith("Map("):
        ktype, vtype = _split_args(ctype[4:-1])
        return "Map(" + _wire_type(ktype) + ", " + _wire_type(vtype) + ")"
    if ctype.startswith("Tuple("):
        return (
            "Tuple("
            + ", ".join(_wire_element(s) for s in _split_args(ctype[6:-1]))
            + ")"
        )
    return ctype


def _wire_element(spec):
    """Rewrite one (possibly ``name Type``) Tuple element to its wire form."""
    etype = _element_type(spec)
    if etype == spec:
        return _wire_type(spec)
    name = spec[: len(spec) - len(etype)].rstrip()
    return name + " " + _wire_type(etype)


def _zero_value(ctype):
    """A valid default for an inner type, used to fill NULL slots.

    In a Native Nullable column every row carries a value (the null map decides
    which are NULL), so NULL positions still need encodable bytes; the value
    itself is discarded on read.
    """
    if ctype.startswith("Array("):
        return []
    if ctype.startswith("Map("):
        return {}
    if ctype.startswith("Tuple("):
        return tuple(_zero_value(_element_type(s)) for s in _split_args(ctype[6:-1]))
    if ctype.startswith("Float"):
        return 0.0
    if ctype == "String" or ctype.startswith("FixedString"):
        return ""
    if ctype == "Bool":
        return False
    if ctype.startswith("DateTime"):
        return _EPOCH_DATETIME
    if ctype.startswith("Date"):
        return _EPOCH_DATE
    if ctype.startswith("Decimal"):
        return Decimal(0)
    if ctype == "UUID":
        return UUID(int=0)
    if ctype.startswith("IPv4"):
        return IPv4Address(0)
    if ctype.startswith("IPv6"):
        return IPv6Address(0)
    if ctype.startswith("Enum"):
        match = re.search(r"'((?:[^'\\]|\\.)*)'", ctype)
        return match.group(1) if match else ""
    return 0  # Int/UInt of any width


def _fill_nulls(values, inner):
    """Replace None with a placeholder for the Nullable values sub-column."""
    sample = next((v for v in values if v is not None), None)
    if sample is None:
        sample = _zero_value(inner)
    return [sample if v is None else v for v in values]


def encode_column(values, ctype):
    """Encode a list of ``values`` as one Native column body of type ``ctype``."""
    spec = _NUMERIC.get(ctype)
    if spec is not None:
        column = array.array(spec[0], values)
        if not _LE:
            column.byteswap()
        return column.tobytes()
    if ctype == "String":
        # Bulk varint-prefixed UTF-8, far cheaper than the per-value writer.
        return write_string_column(values)
    if ctype == "Date":
        column = array.array("H", [(v - _EPOCH_DATE).days for v in values])
        if not _LE:
            column.byteswap()
        return column.tobytes()
    if ctype == "DateTime":
        # Naive UTC / aware seconds (matches the no-timezone DateTime writer).
        # Timezone DateTime / DateTime64 keep the per-value writer path below.
        column = array.array(
            "I", [to_epoch_micros(v, None) // 1_000_000 for v in values]
        )
        if not _LE:
            column.byteswap()
        return column.tobytes()
    if ctype.startswith("Nullable("):
        inner = ctype[9:-1]
        flags = bytes(1 if v is None else 0 for v in values)
        return flags + encode_column(_fill_nulls(values, inner), inner)
    if ctype.startswith("Array("):
        inner = ctype[6:-1]
        offsets = array.array("Q")
        flat = []
        total = 0
        for value in values:
            total += len(value)
            offsets.append(total)
            flat.extend(value)
        if not _LE:
            offsets.byteswap()
        return offsets.tobytes() + encode_column(flat, inner)
    if ctype.startswith("Tuple("):
        specs = [_element_type(s) for s in _split_args(ctype[6:-1])]
        return b"".join(
            encode_column([row[i] for row in values], etype)
            for i, etype in enumerate(specs)
        )
    if ctype.startswith("Map("):
        ktype, vtype = _split_args(ctype[4:-1])
        offsets = array.array("Q")
        keys = []
        vals = []
        total = 0
        for mapping in values:
            total += len(mapping)
            offsets.append(total)
            keys.extend(mapping.keys())
            vals.extend(mapping.values())
        if not _LE:
            offsets.byteswap()
        return (
            offsets.tobytes() + encode_column(keys, ktype) + encode_column(vals, vtype)
        )
    # Scalar fallback: a Native column is its RowBinary per-value writes back to
    # back (numerics excepted above for the array fast path).
    writer = what_py_type(ctype)
    return b"".join(writer.write(value) for value in values)


# Default rows per streamed Native block on INSERT (the client exposes this as
# ``insert_block_size``). Smaller blocks overlap the server-side insert with the
# next block's encoding more finely, at the cost of slightly more per-block
# framing and a smaller atomic unit; a few thousand rows is a good middle ground.
_INSERT_BLOCK_ROWS = 8192


def _encode_block(rows, names, wire_types):
    parts = [_write_varint(len(names)), _write_varint(len(rows))]
    columns = list(zip(*rows)) if rows else [() for _ in names]
    for name, wire_type, column in zip(names, wire_types, columns):
        parts.append(_write_str(name))
        parts.append(_write_str(wire_type))
        parts.append(encode_column(list(column), wire_type))
    return b"".join(parts)


def rows_to_native(rows, names, types):
    """Encode ``rows`` (iterables of column values) as a single Native block."""
    rows = [tuple(row) for row in rows]
    wire_types = [_wire_type(t) for t in types]
    return _encode_block(rows, names, wire_types)


async def rows_to_native_stream(rows, names, types, block_rows=_INSERT_BLOCK_ROWS):
    """Yield an INSERT body as a sequence of Native blocks (async, for streaming).

    Streaming the body block-by-block lets ClickHouse insert one block while the
    client is still encoding the next, overlapping client-side encoding with the
    server-side insert (a meaningful end-to-end speedup on large inserts). The
    generator is async so it can be handed straight to the HTTP backends as a
    chunked request body.

    Trade-off: a body split into several blocks is **not atomic** on a
    *client-side encoding* error. If encoding raises partway through (e.g. a
    single out-of-range or wrong-typed value somewhere in the rows), the blocks
    already yielded have been sent and inserted, so the table is left with a
    partial result. ``rows`` that fit in one block (``len(rows) <= block_rows``)
    are a single block and keep the all-or-nothing behaviour.
    """
    rows = [tuple(row) for row in rows]
    wire_types = [_wire_type(t) for t in types]
    if len(rows) <= block_rows:
        yield _encode_block(rows, names, wire_types)
        return
    for start in range(0, len(rows), block_rows):
        yield _encode_block(rows[start : start + block_rows], names, wire_types)
