import datetime as dt
import re
from abc import ABC, abstractmethod
from decimal import Decimal
from ipaddress import IPv4Address, IPv6Address
from typing import Any, Callable, Generator, Optional
from uuid import UUID

from aiochclient.exceptions import ChClientError

try:
    import ciso8601

    date_parse = datetime_parse = datetime_parse_f = ciso8601.parse_datetime
except ImportError:

    def date_parse(string):
        return dt.datetime.strptime(string, '%Y-%m-%d')

    def datetime_parse(string):
        return dt.datetime.strptime(string, '%Y-%m-%d %H:%M:%S')

    def datetime_parse_f(string):
        return dt.datetime.strptime(string, '%Y-%m-%d %H:%M:%S.%f')


__all__ = ["what_py_converter", "rows2ch", "json2ch", "py2ch", "empty_convertor"]


RE_TUPLE = re.compile(r"^Tuple\((.*)\)$")
RE_ARRAY = re.compile(r"^Array\((.*)\)$")
RE_NULLABLE = re.compile(r"^Nullable\((.*)\)$")
RE_LOW_CARDINALITY = re.compile(r"^LowCardinality\((.*)\)$")


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
        Generator for parsing tuples and arrays.
        Returns elements one by one
        """
        cur = []
        in_str = False
        in_arr = False
        in_tup = False
        if not raw:
            return None
        for sym in raw:
            if not (in_str or in_arr or in_tup):
                if sym == cls.CM:
                    yield "".join(cur)
                    cur = []
                    continue
                elif sym == cls.DQ:
                    in_str = not in_str
                elif sym == cls.ARR_OP:
                    in_arr = True
                elif sym == cls.TUP_OP:
                    in_tup = True
            elif in_str and sym == cls.DQ:
                in_str = not in_str
            elif in_arr and sym == cls.ARR_CLS:
                in_arr = False
            elif in_tup and sym == cls.TUP_CLS:
                in_tup = False
            cur.append(sym)
        yield "".join(cur)

    def convert(self, value: bytes) -> Any:
        return self.p_type(self.decode(value))

    @staticmethod
    def unconvert(value) -> bytes:
        return b"%a" % value


class StrType(BaseType):
    def p_type(self, string: str):
        if self.container:
            return string.strip("'")
        return string

    @staticmethod
    def unconvert(value: str) -> bytes:
        value = value.replace("\\", "\\\\").replace("'", "\\'")
        return f"'{value}'".encode()


class IntType(BaseType):
    p_type = int

    def convert(self, value: bytes) -> Any:
        return self.p_type(value)

    @staticmethod
    def unconvert(value: int) -> bytes:
        return b"%d" % value


class FloatType(IntType):
    p_type = float

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

    @staticmethod
    def unconvert(value: dt.date) -> bytes:
        return b"%a" % str(value)


class DateTimeType(BaseType):
    def p_type(self, string: str):
        string = string.strip("'")
        try:
            return datetime_parse(string)
        except ValueError:
            # In case of 0000-00-00 00:00:00
            if string == "0000-00-00 00:00:00":
                return None
            raise

    def convert(self, value: bytes) -> Optional[dt.datetime]:
        return self.p_type(value.decode())

    @staticmethod
    def unconvert(value: dt.datetime) -> bytes:
        return b"%a" % dt.datetime.strftime(value, '%Y-%m-%d %H:%M:%S')


class DateTime64Type(BaseType):
    def p_type(self, string: str):
        string = string.strip("'")
        try:
            return datetime_parse_f(string)
        except ValueError:
            # In case of 0000-00-00 00:00:00
            if string == "0000-00-00 00:00:00.000":
                return None
            raise

    def convert(self, value: bytes) -> Optional[dt.datetime]:
        return self.p_type(value.decode())


class UUIDType(BaseType):
    def p_type(self, string):
        return UUID(string.strip("'"))

    def convert(self, value: bytes) -> UUID:
        return self.p_type(value.decode())

    @staticmethod
    def unconvert(value: UUID) -> bytes:
        return b"%a" % str(value)


class IPv4Type(BaseType):
    def p_type(self, string):
        return IPv4Address(string.strip("'"))

    def convert(self, value: bytes) -> IPv4Address:
        return self.p_type(value.decode())

    @staticmethod
    def unconvert(value: UUID) -> bytes:
        return b"%a" % str(value)


class IPv6Type(BaseType):
    def p_type(self, string):
        return IPv6Address(string.strip("'"))

    def convert(self, value: bytes) -> IPv6Address:
        return self.p_type(value.decode())

    @staticmethod
    def unconvert(value: UUID) -> bytes:
        return b"%a" % str(value)


class TupleType(BaseType):

    __slots__ = ("name", "types")

    def __init__(self, name: str, **kwargs):
        super().__init__(name, **kwargs)
        tps = RE_TUPLE.findall(name)[0]
        self.types = tuple(what_py_type(tp, container=True) for tp in tps.split(","))

    def p_type(self, string: str) -> tuple:
        return tuple(
            tp.p_type(val)
            for tp, val in zip(self.types, self.seq_parser(string.strip("()")))
        )

    @staticmethod
    def unconvert(value) -> bytes:
        return b"(" + b",".join(py2ch(elem) for elem in value) + b")"


class ArrayType(BaseType):

    __slots__ = ("name", "type")

    def __init__(self, name: str, **kwargs):
        super().__init__(name, **kwargs)
        self.type = what_py_type(RE_ARRAY.findall(name)[0], container=True)

    def p_type(self, string: str) -> list:
        return [self.type.p_type(val) for val in self.seq_parser(string[1:-1])]

    @staticmethod
    def unconvert(value) -> bytes:
        return b"[" + b",".join(py2ch(elem) for elem in value) + b"]"


class NullableType(BaseType):

    __slots__ = ("name", "type")
    NULLABLE = {r"\N", "NULL"}

    def __init__(self, name: str, **kwargs):
        super().__init__(name, **kwargs)
        self.type = what_py_type(RE_NULLABLE.findall(name)[0])

    def p_type(self, string: str) -> Any:
        if string in self.NULLABLE:
            return None
        return self.type.p_type(string)

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

    def __init__(self, name: str, **kwargs):
        super().__init__(name, **kwargs)
        self.type = what_py_type(RE_LOW_CARDINALITY.findall(name)[0])

    def p_type(self, string: str) -> Any:
        return self.type.p_type(string)


class DecimalType(BaseType):
    p_type = Decimal

    def convert(self, value: bytes) -> Decimal:
        return self.p_type(value.decode())

    @staticmethod
    def unconvert(value: Decimal) -> bytes:
        return str(value).encode()


CH_TYPES_MAPPING = {
    "UInt8": IntType,
    "UInt16": IntType,
    "UInt32": IntType,
    "UInt64": IntType,
    "Int8": IntType,
    "Int16": IntType,
    "Int32": IntType,
    "Int64": IntType,
    "Float32": FloatType,
    "Float64": FloatType,
    "String": StrType,
    "FixedString": StrType,
    "Enum8": StrType,
    "Enum16": StrType,
    "Date": DateType,
    "DateTime": DateTimeType,
    "DateTime64": DateTime64Type,
    "Tuple": TupleType,
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
}

PY_TYPES_MAPPING = {
    int: IntType.unconvert,
    float: FloatType.unconvert,
    str: StrType.unconvert,
    dt.date: DateType.unconvert,
    dt.datetime: DateTimeType.unconvert,
    tuple: TupleType.unconvert,
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


def py2ch(value):
    try:
        return PY_TYPES_MAPPING[type(value)](value)
    except KeyError:
        raise ChClientError(
            f"Unrecognized type: '{type(value)}'. "
            f"The value type should be exactly one of "
            f"int, float, str, dt.date, dt.datetime, tuple, list, uuid.UUID (or None). "
            f"No subclasses yet."
        )


def rows2ch(*rows):
    return b",".join(TupleType.unconvert(row) for row in rows)


def json2ch(*records, dumps: Callable[[Any], bytes]):
    return dumps(records)[1:-1]


def empty_convertor(value: bytes) -> bytes:
    return value
