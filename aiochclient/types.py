from typing import Generator, Any, Type
import re
import datetime as dt
from aiochclient.exceptions import ChClientError
from aiochclient.parsers import seq_parser, decode

__all__ = ["what_py_type", "rows2ch"]


class BaseType:

    __slots__ = ("name", "container")

    ESC_CHR_MAPPING = {
        b"b": b"\b",
        b"N": b"\N",  # NULL
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

    def __init__(self, name: str, container: bool = False):
        self.name = name
        self.container = container

    def p_type(self, string):
        """ Function for implementing specific actions for each type """
        return string

    @classmethod
    def _decode(cls, val: bytes) -> str:
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

    decode = decode

    @classmethod
    def _seq_parser(cls, raw: str) -> Generator[str, None, None]:
        """
        Generator for parsing tuples and arrays.
        Returns elements one by one
        """
        cur = []
        blocked = False
        if not raw:
            return None
        for sym in raw:
            if sym == cls.CM and not blocked:
                yield "".join(cur)
                cur = []
            elif sym == cls.DQ:
                blocked = not blocked
                cur.append(sym)
            else:
                cur.append(sym)
        yield "".join(cur)

    seq_parser = seq_parser

    def convert(self, value: bytes) -> Any:
        return self.p_type(self.decode(value))

    @staticmethod
    def unconvert(value) -> bytes:
        return f"'{value}'".encode()


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
    def p_type(self, string: str):
        return int(string)

    @staticmethod
    def unconvert(value) -> bytes:
        return f"{value}".encode()


class FloatType(IntType):
    def p_type(self, string: str):
        return float(string)


class DateType(BaseType):
    def p_type(self, string: str):
        string = string.strip("'")
        try:
            return dt.datetime.strptime(string, "%Y-%m-%d").date()
        except ValueError:
            # In case of 0000-00-00
            if string == "0000-00-00":
                return None
            raise


class DateTimeType(BaseType):
    def p_type(self, string: str):
        string = string.strip("'")
        try:
            return dt.datetime.strptime(string, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            # In case of 0000-00-00 00:00:00
            if string == "0000-00-00 00:00:00":
                return None
            raise

    @staticmethod
    def unconvert(value) -> bytes:
        return f"'{value.replace(microsecond=0)}'".encode()


class TupleType(BaseType):

    __slots__ = ("name", "types")

    def __init__(self, name: str, **kwargs):
        super().__init__(name, **kwargs)
        tps = re.findall(r"^Tuple\((.*)\)$", name)[0]
        self.types = tuple(what_py_type(tp, container=True) for tp in tps.split(","))

    def p_type(self, string: str):
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
        self.type = what_py_type(
            re.findall(r"^Array\((.*)\)$", name)[0], container=True
        )

    def p_type(self, string: str):
        return [self.type.p_type(val) for val in self.seq_parser(string.strip("[]"))]

    @staticmethod
    def unconvert(value) -> bytes:
        return b"[" + b",".join(py2ch(elem) for elem in value) + b"]"


class NullableType(BaseType):

    __slots__ = ("name", "type")

    def __init__(self, name: str, **kwargs):
        super().__init__(name, **kwargs)
        self.type = what_py_type(re.findall(r"^Nullable\((.*)\)$", name)[0])

    def p_type(self, string: str):
        if string in {r"\N", "NULL"}:
            return None
        return self.type.p_type(string)

    @staticmethod
    def unconvert(value) -> bytes:
        return b"NULL"


class NothingType(BaseType):
    def p_type(self, string: str):
        return None


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
    "Tuple": TupleType,
    "Array": ArrayType,
    "Nullable": NullableType,
    "Nothing": NothingType,
}

PY_TYPES_MAPPING = {
    int: IntType,
    float: FloatType,
    str: StrType,
    dt.date: DateType,
    dt.datetime: DateTimeType,
    tuple: TupleType,
    list: ArrayType,
    type(None): NullableType,
}


def what_py_type(name: str, container: bool = False) -> BaseType:
    """ Returns needed type class from clickhouse type name """
    name = name.strip()
    try:
        return CH_TYPES_MAPPING[name.split("(")[0]](name, container=container)
    except KeyError:
        raise ChClientError(f"Unrecognized type name: '{name}'")


def py2ch(value):
    return what_ch_type(type(value)).unconvert(value)


def rows2ch(*rows):
    return b",".join(TupleType.unconvert(row) for row in rows)


def what_ch_type(typ) -> Type[BaseType]:
    try:
        return PY_TYPES_MAPPING[typ]
    except KeyError:
        raise ChClientError(
            f"Unrecognized type: '{typ}'. "
            f"The value type should be exactly one of "
            f"int, float, str, dt.date, dt.datetime, tuple, list (or None). "
            f"No subclasses yet."
        )
