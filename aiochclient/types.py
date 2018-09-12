from typing import Generator
import re
from datetime import datetime as dt
from aiochclient.exceptions import ChClientError

__all__ = ["what_type"]


class BaseType:

    __slots__ = ("name",)

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

    def __init__(self, name: str):
        self.name = name

    def p_type(self, string: str):
        """ Function for implementing specific actions for each type """
        string = string.strip("'")
        return string

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
        blocked = False
        if not raw:
            raise StopIteration
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

    def convert(self, value: bytes):
        return self.p_type(self.decode(value))


class StrType(BaseType):
    pass


class IntType(BaseType):
    def p_type(self, string: str):
        return int(string)


class FloatType(BaseType):
    def p_type(self, string: str):
        return float(string)


class DateType(BaseType):
    def p_type(self, string: str):
        string = string.strip("'")
        try:
            return dt.strptime(string, "%Y-%m-%d").date()
        except ValueError:
            # In case of 0000-00-00
            if string == "0000-00-00":
                return None
            raise


class DateTimeType(BaseType):
    def p_type(self, string: str):
        string = string.strip("'")
        try:
            return dt.strptime(string, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            # In case of 0000-00-00 00:00:00
            if string == "0000-00-00 00:00:00":
                return None
            raise


class TupleType(BaseType):

    __slots__ = ("name", "types")

    def __init__(self, name: str):
        super().__init__(name)
        tps = re.findall(r"^Tuple\((.*)\)$", name)[0]
        self.types = tuple(what_type(tp) for tp in tps.split(","))

    def p_type(self, string: str):
        return tuple(
            tp.p_type(val)
            for tp, val in zip(self.types, self.seq_parser(string.strip("()")))
        )


class ArrayType(BaseType):

    __slots__ = ("name", "type")

    def __init__(self, name: str):
        super().__init__(name)
        self.type = what_type(re.findall(r"^Array\((.*)\)$", name)[0])

    def p_type(self, string: str):
        return [self.type.p_type(val) for val in self.seq_parser(string.strip("[]"))]


class NullableType(BaseType):

    __slots__ = ("name", "type")

    def __init__(self, name: str):
        super().__init__(name)
        self.type = what_type(re.findall(r"^Nullable\((.*)\)$", name)[0])

    def p_type(self, string: str):
        if string in {r"\N", "NULL"}:
            return None
        return self.type.p_type(string)


class NothingType(BaseType):
    def p_type(self, string: str):
        return None


TYPES_MAPPING = {
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


def what_type(name: str) -> BaseType:
    """ Returns needed type class from clickhouse type name """
    name = name.strip()
    try:
        return TYPES_MAPPING[name.split("(")[0]](name)
    except KeyError:
        raise ChClientError(f"Unrecognized type name: '{name}'")
