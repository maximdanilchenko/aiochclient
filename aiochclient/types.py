import datetime as dt
import re
import struct
from abc import ABC, abstractmethod
from decimal import Decimal
from ipaddress import IPv4Address, IPv6Address
from typing import Any, Callable, Generator, List, Optional, Tuple
from uuid import UUID
from zoneinfo import ZoneInfo

from aiochclient.exceptions import ChClientError, NeedMoreData


class Cursor:
    """Synchronous forward cursor over a bytes buffer (RowBinary engine)."""

    __slots__ = ("buf", "pos")

    def __init__(self, buf: bytes = b"", pos: int = 0):
        self.buf = buf
        self.pos = pos

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

    def read_int(self, size: int, signed: bool) -> int:
        return int.from_bytes(self.read(size), "little", signed=signed)

    def read_double(self) -> float:
        return struct.unpack("<d", self.read(8))[0]

    def read_float(self) -> float:
        return struct.unpack("<f", self.read(4))[0]

    # -- Native engine: whole-column bulk reads (pure-Python fallback) --

    def read_string_column(self, n: int) -> list:
        buf = self.buf
        size = len(buf)
        pos = self.pos
        out = []
        for _ in range(n):
            length = 0
            shift = 0
            while True:
                if pos >= size:
                    raise NeedMoreData
                byte = buf[pos]
                pos += 1
                length |= (byte & 0x7F) << shift
                if not byte & 0x80:
                    break
                shift += 7
            end = pos + length
            if end > size:
                raise NeedMoreData
            out.append(buf[pos:end].decode())
            pos = end
        self.pos = pos
        return out

    def read_date_column(self, n: int) -> list:
        buf = self.buf
        pos = self.pos
        end = pos + n * 2
        if end > len(buf):
            raise NeedMoreData
        out = [
            RB_EPOCH_DATE
            + dt.timedelta(days=buf[pos + 2 * i] | (buf[pos + 2 * i + 1] << 8))
            for i in range(n)
        ]
        self.pos = end
        return out

    def read_datetime_column(self, n: int) -> list:
        buf = self.buf
        pos = self.pos
        end = pos + n * 4
        if end > len(buf):
            raise NeedMoreData
        out = []
        for i in range(n):
            p = pos + 4 * i
            secs = buf[p] | (buf[p + 1] << 8) | (buf[p + 2] << 16) | (buf[p + 3] << 24)
            out.append(RB_EPOCH_DATETIME + dt.timedelta(seconds=secs))
        self.pos = end
        return out


_TZ_UNSET = object()


def _parse_tz(name: str) -> Optional[str]:
    """Extract the timezone from a ``DateTime``/``DateTime64`` type name, if any.

    e.g. ``DateTime64(3, 'Europe/Moscow')`` / ``DateTime('UTC')`` -> the tz name.
    The TSV header escapes the quotes (``\\'Europe/Moscow\\'``), so backslashes
    are stripped out.
    """
    if "'" in name:
        return name[name.index("'") + 1 : name.rindex("'")].replace("\\", "")
    return None


# RowBinary integer specs: name -> (byte width, signed). All little-endian.
RB_INT_SPECS = {
    "UInt8": (1, False),
    "UInt16": (2, False),
    "UInt32": (4, False),
    "UInt64": (8, False),
    "UInt128": (16, False),
    "UInt256": (32, False),
    "Int8": (1, True),
    "Int16": (2, True),
    "Int32": (4, True),
    "Int64": (8, True),
    "Int128": (16, True),
    "Int256": (32, True),
}

# Epoch used to turn day/second/tick offsets into python date/datetime objects.
RB_EPOCH_DATE = dt.date(1970, 1, 1)
RB_EPOCH_DATETIME = dt.datetime(1970, 1, 1)
RB_EPOCH_UTC = dt.datetime(1970, 1, 1, tzinfo=dt.timezone.utc)
_MICROSECOND = dt.timedelta(microseconds=1)


def to_epoch_micros(value: dt.datetime, zone) -> int:
    """Microseconds since the epoch for a naive or aware ``value``.

    Naive + column zone -> wall-clock in that zone. Naive + no zone -> UTC
    wall-clock (the historical behaviour). Aware -> its real instant, whatever
    the column zone is.
    """
    if value.tzinfo is None:
        if zone is None:
            return (value - RB_EPOCH_DATETIME) // _MICROSECOND
        value = value.replace(tzinfo=zone)
    return (value - RB_EPOCH_UTC) // _MICROSECOND


def write_varint(value: int) -> bytes:
    """Encode an unsigned integer as LEB128 (RowBinary length prefix)."""
    out = bytearray()
    while True:
        byte = value & 0x7F
        value >>= 7
        if value:
            out.append(byte | 0x80)
        else:
            out.append(byte)
            return bytes(out)


try:
    import ciso8601

    date_parse = datetime_parse = datetime_parse_f = ciso8601.parse_datetime
except ImportError:

    def date_parse(string):
        return dt.datetime.strptime(string, '%Y-%m-%d')

    def datetime_parse(string):
        return dt.datetime.strptime(string, '%Y-%m-%d %H:%M:%S')

    def datetime_parse_f(string):
        # ClickHouse DateTime64 may carry up to 9 fractional digits
        # (nanoseconds), but Python datetime only supports microseconds and
        # strptime's "%f" rejects more than 6 digits. Truncate the fractional
        # part to microseconds so DateTime64(7..9) parses — matching the
        # behaviour of ciso8601 when it is installed.
        head, _, frac = string.partition('.')
        parsed = dt.datetime.strptime(head, '%Y-%m-%d %H:%M:%S')
        if frac:
            parsed = parsed.replace(microsecond=int(frac[:6].ljust(6, '0')))
        return parsed


__all__ = ["what_py_converter", "rows2ch", "json2ch", "py2ch", "empty_convertor"]


RE_TUPLE = re.compile(r"^Tuple\((.*)\)$")
RE_ARRAY = re.compile(r"^Array\((.*)\)$")
RE_NESTED = re.compile(r"^Nested\((.*)\)$")
RE_NULLABLE = re.compile(r"^Nullable\((.*)\)$")
RE_LOW_CARDINALITY = re.compile(r"^LowCardinality\((.*)\)$")
RE_MAP = re.compile(r"^Map\((.*)\)$")
RE_REPLACE_QUOTE = re.compile(r"(?<!\\)'")


def remove_single_quotes(string: str) -> str:
    if string[0] == string[-1] == "'":
        return string[1:-1]
    return string


class BaseType(ABC):
    __slots__ = ("name", "container")

    ESC_CHR_MAPPING = {
        b"b": b"\b",
        b"N": b"\\N",  # NULL
        b"f": b"\f",
        b"r": b"\r",
        b"n": b"\n",
        b"t": b"\t",
        b"0": b" ",
        b"'": b"'",
        b"\\": b"\\",
    }

    DQ = "'"
    CM = ","
    ESCAPE_OP = '\\'
    TUP_OP = '('
    TUP_CLS = ')'
    ARR_OP = '['
    ARR_CLS = ']'

    def __init__(self, name: str, container: bool = False):
        self.name = name
        self.container = container

    @abstractmethod
    def p_type(self, string):
        """Function for implementing specific actions for each type"""

    @classmethod
    def decode(cls, val: bytes) -> str:
        """
        Converting bytes from clickhouse with
        backslash-escaped special characters
        to pythonic string format
        """
        n = val.find(b"\\")
        if n < 0:
            return val.decode()
        n += 1
        d = val[:n]
        b = val[n:]
        while b:
            d = d[:-1] + cls.ESC_CHR_MAPPING.get(b[0:1], b[0:1])
            b = b[1:]
            n = b.find(b"\\")
            if n < 0:
                d = d + b
                break
            n += 1
            d = d + b[:n]
            b = b[n:]
        return d.decode()

    @classmethod
    def seq_parser(cls, raw: str) -> Generator[str, None, None]:
        """
        Generator for parsing the body of tuples, arrays and maps.

        Yields the top-level comma-separated elements one by one, keeping
        quoted strings and nested brackets intact, so structural characters
        like ``,``, ``(``, ``)``, ``[`` or ``]`` inside a quoted string are
        not treated as separators.
        """
        if not raw:
            return
        cur = []
        depth = 0
        in_str = False
        escape_char = False
        for sym in raw:
            if in_str:
                cur.append(sym)
                if escape_char:
                    escape_char = False
                elif sym == cls.ESCAPE_OP:
                    escape_char = True
                elif sym == cls.DQ:
                    in_str = False
                continue
            if sym == cls.DQ:
                in_str = True
            elif sym == cls.ARR_OP or sym == cls.TUP_OP:
                depth += 1
            elif sym == cls.ARR_CLS or sym == cls.TUP_CLS:
                depth -= 1
            elif sym == cls.CM and depth == 0:
                yield "".join(cur)
                cur.clear()
                continue
            cur.append(sym)
        yield "".join(cur)

    def convert(self, value: bytes) -> Any:
        return self.p_type(self.decode(value))

    def read(self, cursor) -> Any:
        """Read a single value from a RowBinary cursor (binary engine)."""
        raise ChClientError(
            f"RowBinary decoding is not implemented for type '{self.name}'"
        )

    def write(self, value) -> bytes:
        """Encode a single value to RowBinary bytes (binary engine)."""
        raise ChClientError(
            f"RowBinary encoding is not implemented for type '{self.name}'"
        )

    @staticmethod
    def unconvert(value) -> bytes:
        return b"%a" % value


class StrType(BaseType):
    def p_type(self, string: str) -> str:
        string = self.decode(string.encode())
        if self.container:
            return remove_single_quotes(string)
        return string

    def convert(self, value: bytes) -> str:
        # ``value`` is the raw, still backslash-escaped bytes from ClickHouse.
        # ``p_type`` does the (single) escape-decoding, so we only utf-8 decode
        # here — decoding twice would re-interpret escape sequences and, for
        # example, turn a literal ``\t`` into a tab or drop a trailing backslash.
        return self.p_type(value.decode())

    def read(self, cursor) -> str:
        if self.name.startswith("FixedString"):
            length = int(self.name[12:-1])
        else:
            length = cursor.read_varint()
        return cursor.read(length).decode()

    def write(self, value: str) -> bytes:
        data = value.encode()
        if self.name.startswith("FixedString"):
            length = int(self.name[12:-1])
            return data[:length].ljust(length, b"\x00")
        return write_varint(len(data)) + data

    @staticmethod
    def unconvert(value: str) -> bytes:
        value = value.replace("\\", "\\\\").replace("'", "\\'")
        return f"'{value}'".encode()


class BoolType(BaseType):
    def p_type(self, string) -> bool:
        if string == "true":
            return True
        elif string == "false":
            return False
        else:
            raise ValueError("invalid boolean value: {!r}".format(string))

    def convert(self, value: bytes) -> bool:
        return self.p_type(value.decode())

    def read(self, cursor) -> bool:
        return cursor.read_int(1, False) != 0

    def write(self, value: bool) -> bytes:
        return b"\x01" if value else b"\x00"

    @staticmethod
    def unconvert(value: bool) -> bytes:
        # We use an integer representation here to be compatible with older
        # ClickHouse versions that represent booleans as UInt8 values.
        return b"1" if value else b"0"


class IntType(BaseType):
    p_type = int

    def convert(self, value: bytes) -> Any:
        return self.p_type(value)

    def read(self, cursor) -> int:
        size, signed = RB_INT_SPECS[self.name]
        return cursor.read_int(size, signed)

    def write(self, value: int) -> bytes:
        size, signed = RB_INT_SPECS[self.name]
        return int(value).to_bytes(size, "little", signed=signed)

    @staticmethod
    def unconvert(value: int) -> bytes:
        return b"%d" % value


class FloatType(IntType):
    p_type = float

    def read(self, cursor) -> float:
        if self.name == "Float64":
            return cursor.read_double()
        return cursor.read_float()

    def write(self, value: float) -> bytes:
        if self.name == "Float64":
            return struct.pack("<d", value)
        return struct.pack("<f", value)

    @staticmethod
    def unconvert(value: float) -> bytes:
        return b"%r" % value


class DateType(BaseType):
    def p_type(self, string: str):
        string = string.strip("'")
        try:
            return date_parse(string).date()
        except ValueError:
            # In case of 0000-00-00
            if string == "0000-00-00":
                return None
            raise

    def convert(self, value: bytes) -> Optional[dt.date]:
        return self.p_type(value.decode())

    def read(self, cursor) -> dt.date:
        return RB_EPOCH_DATE + dt.timedelta(days=cursor.read_int(2, False))

    def write(self, value: dt.date) -> bytes:
        return (value - RB_EPOCH_DATE).days.to_bytes(2, "little")

    @staticmethod
    def unconvert(value: dt.date) -> bytes:
        return b"%a" % str(value)


class DateTimeType(BaseType):
    def __init__(self, name: str, container: bool = False):
        super().__init__(name, container)
        # Resolve the (optional) timezone lazily on the first binary read so the
        # TSV path — which never calls ``read`` and sees escaped type names — is
        # untouched and ``tzdata`` is only required when actually decoding tz.
        self._tz_name = _parse_tz(name)
        self._tz = _TZ_UNSET

    def _zone(self):
        if self._tz is _TZ_UNSET:
            self._tz = ZoneInfo(self._tz_name) if self._tz_name else None
        return self._tz

    def p_type(self, string: str):
        string = string.strip("'")
        try:
            value = datetime_parse(string)
        except ValueError:
            # In case of 0000-00-00 00:00:00
            if string == "0000-00-00 00:00:00":
                return None
            raise
        zone = self._zone()
        return value.replace(tzinfo=zone) if zone else value

    def convert(self, value: bytes) -> Optional[dt.datetime]:
        return self.p_type(value.decode())

    def read(self, cursor) -> dt.datetime:
        seconds = cursor.read_int(4, False)
        zone = self._zone()
        if zone is None:
            return RB_EPOCH_DATETIME + dt.timedelta(seconds=seconds)
        return dt.datetime.fromtimestamp(seconds, zone)

    def write(self, value: dt.datetime) -> bytes:
        seconds = to_epoch_micros(value, self._zone()) // 1_000_000
        return seconds.to_bytes(4, "little")

    @staticmethod
    def unconvert(value: dt.datetime) -> bytes:
        # str() keeps the sub-second part only when present (a DateTime column
        # rejects it) and the UTC offset of an aware value, which ClickHouse
        # applies. Same as the compiled ``unconvert_datetime``.
        return b"'%s'" % str(value).encode()


class DateTime64Type(BaseType):
    def __init__(self, name: str, container: bool = False):
        super().__init__(name, container)
        # DateTime64(P) or DateTime64(P, 'TZ') -> precision P (+ optional tz)
        self._precision = int(name[name.index("(") + 1 :].split(",")[0].split(")")[0])
        self._tz_name = _parse_tz(name)
        self._tz = _TZ_UNSET

    def _zone(self):
        if self._tz is _TZ_UNSET:
            self._tz = ZoneInfo(self._tz_name) if self._tz_name else None
        return self._tz

    def p_type(self, string: str):
        string = string.strip("'")
        try:
            value = datetime_parse_f(string)
        except ValueError:
            # In case of 0000-00-00 00:00:00
            if string == "0000-00-00 00:00:00.000":
                return None
            raise
        zone = self._zone()
        return value.replace(tzinfo=zone) if zone else value

    def convert(self, value: bytes) -> Optional[dt.datetime]:
        return self.p_type(value.decode())

    def read(self, cursor) -> dt.datetime:
        ticks = cursor.read_int(8, True)
        # Python datetime only supports microseconds; truncate finer precision.
        if self._precision <= 6:
            micros = ticks * 10 ** (6 - self._precision)
        else:
            micros = ticks // 10 ** (self._precision - 6)
        zone = self._zone()
        if zone is None:
            return RB_EPOCH_DATETIME + dt.timedelta(microseconds=micros)
        return (RB_EPOCH_UTC + dt.timedelta(microseconds=micros)).astimezone(zone)

    def write(self, value: dt.datetime) -> bytes:
        micros = to_epoch_micros(value, self._zone())
        if self._precision <= 6:
            ticks = micros // 10 ** (6 - self._precision)
        else:
            ticks = micros * 10 ** (self._precision - 6)
        return ticks.to_bytes(8, "little", signed=True)


class UUIDType(BaseType):
    def p_type(self, string):
        return UUID(string.strip("'"))

    def convert(self, value: bytes) -> UUID:
        return self.p_type(value.decode())

    def read(self, cursor) -> UUID:
        data = cursor.read(16)
        # Stored as two little-endian UInt64 halves (high, then low).
        return UUID(
            int=(int.from_bytes(data[:8], "little") << 64)
            | int.from_bytes(data[8:], "little")
        )

    def write(self, value: UUID) -> bytes:
        if not isinstance(value, UUID):
            value = UUID(str(value))
        number = value.int
        return (number >> 64).to_bytes(8, "little") + (
            number & 0xFFFFFFFFFFFFFFFF
        ).to_bytes(8, "little")

    @staticmethod
    def unconvert(value: UUID) -> bytes:
        return b"%a" % str(value)


class IPv4Type(BaseType):
    def p_type(self, string):
        return IPv4Address(string.strip("'"))

    def convert(self, value: bytes) -> IPv4Address:
        return self.p_type(value.decode())

    def read(self, cursor) -> IPv4Address:
        return IPv4Address(cursor.read_int(4, False))

    def write(self, value) -> bytes:
        return int(IPv4Address(value)).to_bytes(4, "little")

    @staticmethod
    def unconvert(value: UUID) -> bytes:
        return b"%a" % str(value)


class IPv6Type(BaseType):
    def p_type(self, string):
        return IPv6Address(string.strip("'"))

    def convert(self, value: bytes) -> IPv6Address:
        return self.p_type(value.decode())

    def read(self, cursor) -> IPv6Address:
        return IPv6Address(cursor.read(16))

    def write(self, value) -> bytes:
        return IPv6Address(value).packed

    @staticmethod
    def unconvert(value: UUID) -> bytes:
        return b"%a" % str(value)


class TupleType(BaseType):
    __slots__ = ("name", "types")

    def __init__(self, name: str, **kwargs):
        super().__init__(name, **kwargs)
        tps = RE_TUPLE.findall(name)[0]
        self.types = tuple(
            what_py_type(tp.rpartition(" ")[2], container=True) for tp in tps.split(",")
        )

    def p_type(self, string: str) -> tuple:
        return tuple(
            tp.p_type(val)
            for tp, val in zip(self.types, self.seq_parser(string.strip("()")))
        )

    def convert(self, value: bytes) -> list:
        return self.p_type(value.decode())

    def read(self, cursor) -> tuple:
        return tuple(tp.read(cursor) for tp in self.types)

    def write(self, value) -> bytes:
        return b"".join(tp.write(elem) for tp, elem in zip(self.types, value))

    @staticmethod
    def unconvert(value) -> bytes:
        return b"(" + b",".join(py2ch(elem) for elem in value) + b")"


class MapType(BaseType):
    __slots__ = ("name", "key_type", "value_type")

    def __init__(self, name: str, **kwargs):
        super().__init__(name, **kwargs)
        tps = RE_MAP.findall(name)[0]
        comma_index = tps.index(",")
        self.key_type = what_py_type(tps[:comma_index], container=True)
        self.value_type = what_py_type(tps[comma_index + 1 :], container=True)

    def p_type(self, string: str) -> dict:
        result = {}
        for pair in self.seq_parser(string[1:-1]):
            key, value = self._split_kv(pair)
            result[self.key_type.p_type(key)] = self.value_type.p_type(value)
        return result

    @staticmethod
    def _split_kv(pair: str) -> Tuple[str, str]:
        """Split a ``key:value`` map entry at its first top-level colon."""
        in_str = False
        escape_char = False
        for i, sym in enumerate(pair):
            if in_str:
                if escape_char:
                    escape_char = False
                elif sym == "\\":
                    escape_char = True
                elif sym == "'":
                    in_str = False
            elif sym == "'":
                in_str = True
            elif sym == ":":
                return pair[:i], pair[i + 1 :]
        return pair, ""

    def convert(self, value: bytes) -> dict:
        return self.p_type(value.decode())

    def read(self, cursor) -> dict:
        return {
            self.key_type.read(cursor): self.value_type.read(cursor)
            for _ in range(cursor.read_varint())
        }

    def write(self, value) -> bytes:
        parts = [write_varint(len(value))]
        for key, val in value.items():
            parts.append(self.key_type.write(key))
            parts.append(self.value_type.write(val))
        return b"".join(parts)

    @staticmethod
    def unconvert(value) -> bytes:
        return (
            b"{"
            + b','.join(py2ch(key) + b':' + py2ch(val) for key, val in value.items())
            + b"}"
        )


class ArrayType(BaseType):
    __slots__ = ("name", "type")

    def __init__(self, name: str, **kwargs):
        super().__init__(name, **kwargs)
        self.type = what_py_type(RE_ARRAY.findall(name)[0], container=True)

    def p_type(self, string: str) -> list:
        return [self.type.p_type(val) for val in self.seq_parser(string[1:-1])]

    def convert(self, value: bytes) -> list:
        return self.p_type(value.decode())

    def read(self, cursor) -> list:
        return [self.type.read(cursor) for _ in range(cursor.read_varint())]

    def write(self, value) -> bytes:
        return write_varint(len(value)) + b"".join(
            self.type.write(elem) for elem in value
        )

    @staticmethod
    def unconvert(value) -> bytes:
        return b"[" + b",".join(py2ch(elem) for elem in value) + b"]"


class NestedType(BaseType):
    __slots__ = ("name", "types")

    def __init__(self, name: str, **kwargs):
        super().__init__(name, **kwargs)
        self.types = [
            what_py_type(i.split()[1], container=True)
            for i in RE_NESTED.findall(name)[0].split(',')
        ]

    def p_type(self, string: str) -> List[tuple]:
        return [
            tuple(
                tp.p_type(elem)
                for tp, elem in zip(self.types, self.seq_parser(val.strip("()")))
            )
            for val in self.seq_parser(string[1:-1])
        ]

    def convert(self, value: bytes) -> List[tuple]:
        return self.p_type(value.decode())

    def read(self, cursor) -> List[tuple]:
        # With flatten_nested=0 a Nested column is encoded as Array(Tuple(...)).
        return [
            tuple(tp.read(cursor) for tp in self.types)
            for _ in range(cursor.read_varint())
        ]

    def write(self, value) -> bytes:
        parts = [write_varint(len(value))]
        for row in value:
            for tp, elem in zip(self.types, row):
                parts.append(tp.write(elem))
        return b"".join(parts)

    @staticmethod
    def unconvert(value) -> bytes:
        return (
            b"["
            + b",".join(
                b"(" + b",".join(py2ch(elem) for elem in val) + b")" for val in value
            )
            + b"]"
        )


class NullableType(BaseType):
    __slots__ = ("name", "type")
    NULLABLE = {r"\N", "NULL"}

    def __init__(self, name: str, container: bool = False, **kwargs):
        super().__init__(name, **kwargs)
        self.type = what_py_type(RE_NULLABLE.findall(name)[0], container=container)

    def p_type(self, string: str) -> Any:
        if string in self.NULLABLE:
            return None
        return self.type.p_type(string)

    def read(self, cursor) -> Any:
        if cursor.read(1) != b"\x00":
            return None
        return self.type.read(cursor)

    def write(self, value) -> bytes:
        if value is None:
            return b"\x01"
        return b"\x00" + self.type.write(value)

    @staticmethod
    def unconvert(value) -> bytes:
        return b"NULL"


class NothingType(BaseType):
    def p_type(self, string: str) -> None:
        return None

    def convert(self, value: bytes) -> None:
        return None


class LowCardinalityType(BaseType):
    __slots__ = ("name", "type")

    def __init__(self, name: str, container: bool = False, **kwargs):
        super().__init__(name, **kwargs)
        self.type = what_py_type(
            RE_LOW_CARDINALITY.findall(name)[0], container=container
        )

    def p_type(self, string: str) -> Any:
        return self.type.p_type(string)

    def read(self, cursor) -> Any:
        # In RowBinary a LowCardinality(T) is encoded exactly as T.
        return self.type.read(cursor)

    def write(self, value) -> bytes:
        return self.type.write(value)


class EnumType(StrType):
    """Enum8/Enum16. TSV gives the label string (handled by StrType); RowBinary
    gives the signed integer index, mapped back to its label here."""

    def __init__(self, name: str, container: bool = False):
        super().__init__(name, container)
        self._size = 1 if name.startswith("Enum8") else 2
        self._mapping = {
            int(num): label
            for label, num in re.findall(r"'((?:[^'\\]|\\.)*)'\s*=\s*(-?\d+)", name)
        }
        self._reverse = {label: index for index, label in self._mapping.items()}

    def read(self, cursor) -> str:
        return self._mapping[cursor.read_int(self._size, True)]

    def write(self, value: str) -> bytes:
        return self._reverse[value].to_bytes(self._size, "little", signed=True)


class DecimalType(BaseType):
    p_type = Decimal

    def __init__(self, name: str, container: bool = False):
        super().__init__(name, container)
        nums = [int(n) for n in re.findall(r"\d+", name)]
        if name.startswith("Decimal("):
            precision, self._scale = nums[0], nums[1]
        else:
            precision = {
                "Decimal32": 9,
                "Decimal64": 18,
                "Decimal128": 38,
                "Decimal256": 76,
            }[name.split("(")[0]]
            self._scale = nums[-1]
        self._size = (
            4
            if precision <= 9
            else 8 if precision <= 18 else 16 if precision <= 38 else 32
        )

    def convert(self, value: bytes) -> Decimal:
        return self.p_type(value.decode())

    def read(self, cursor) -> Decimal:
        raw = cursor.read_int(self._size, True)
        return Decimal(raw).scaleb(-self._scale)

    def write(self, value: Decimal) -> bytes:
        raw = int(Decimal(value).scaleb(self._scale))
        return raw.to_bytes(self._size, "little", signed=True)

    @staticmethod
    def unconvert(value: Decimal) -> bytes:
        return str(value).encode()


CH_TYPES_MAPPING = {
    "Bool": BoolType,
    "UInt8": IntType,
    "UInt16": IntType,
    "UInt32": IntType,
    "UInt64": IntType,
    "UInt128": IntType,
    "UInt256": IntType,
    "Int8": IntType,
    "Int16": IntType,
    "Int32": IntType,
    "Int64": IntType,
    "Int128": IntType,
    "Int256": IntType,
    "Float32": FloatType,
    "Float64": FloatType,
    "String": StrType,
    "FixedString": StrType,
    "Enum8": EnumType,
    "Enum16": EnumType,
    "Date": DateType,
    "DateTime": DateTimeType,
    "DateTime64": DateTime64Type,
    "Tuple": TupleType,
    "Map": MapType,
    "Array": ArrayType,
    "Nullable": NullableType,
    "Nothing": NothingType,
    "UUID": UUIDType,
    "LowCardinality": LowCardinalityType,
    "Decimal": DecimalType,
    "Decimal32": DecimalType,
    "Decimal64": DecimalType,
    "Decimal128": DecimalType,
    "IPv4": IPv4Type,
    "IPv6": IPv6Type,
    "Nested": NestedType,
}

PY_TYPES_MAPPING = {
    bool: BoolType.unconvert,
    int: IntType.unconvert,
    float: FloatType.unconvert,
    str: StrType.unconvert,
    dt.date: DateType.unconvert,
    dt.datetime: DateTimeType.unconvert,
    tuple: TupleType.unconvert,
    dict: MapType.unconvert,
    list: ArrayType.unconvert,
    type(None): NullableType.unconvert,
    UUID: UUIDType.unconvert,
    Decimal: DecimalType.unconvert,
    IPv4Address: IPv4Type.unconvert,
    IPv6Address: IPv6Type.unconvert,
}


def what_py_type(name: str, container: bool = False) -> BaseType:
    """Returns needed type class from clickhouse type name"""
    name = name.strip()
    try:
        if name.startswith('SimpleAggregateFunction') or name.startswith(
            'AggregateFunction'
        ):
            ch_type = re.findall(r',(.*)\)', name)[0].strip()
        else:
            ch_type = name.split("(")[0]
        return CH_TYPES_MAPPING[ch_type](name, container=container)
    except KeyError:
        raise ChClientError(f"Unrecognized type name: '{name}'")


def what_py_converter(name: str, container: bool = False) -> Callable:
    """Returns needed type class from clickhouse type name"""
    return what_py_type(name, container).convert


def read_column(cursor, reader, n: int) -> list:
    """Read n contiguous values of one scalar type (Native column fallback).

    Pure-Python mirror of the compiled ``read_column``: a Native scalar column
    is its RowBinary per-value encodings laid out back to back.
    """
    return [reader.read(cursor) for _ in range(n)]


def py2ch(value):
    converter = PY_TYPES_MAPPING.get(type(value))
    if converter is None:
        # Fall back to the closest registered base type, walking the MRO so
        # the most specific match wins (e.g. datetime before date). This lets
        # subclasses of supported types — StrEnum/IntEnum, namedtuples, etc. —
        # be inserted too.
        for base in type(value).__mro__:
            converter = PY_TYPES_MAPPING.get(base)
            if converter is not None:
                break
    if converter is None:
        raise ChClientError(
            f"Unrecognized type: '{type(value)}'. "
            f"The value type should be one of "
            f"int, float, str, dt.date, dt.datetime, "
            f"dict, tuple, list, uuid.UUID (or a subclass of one of them, or None)."
        )
    return converter(value)


def rows2ch(*rows):
    return b",".join(TupleType.unconvert(row) for row in rows)


def json2ch(*records, dumps: Callable[[Any], bytes]):
    return dumps(records)[1:-1]


def empty_convertor(value: bytes) -> bytes:
    return value
