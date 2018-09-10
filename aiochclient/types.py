import re
from datetime import datetime as dt
from abc import ABC, abstractmethod


__all__ = ["what_type", "convert"]


def date(string):
    return dt.strptime(string, "%Y-%m-%d").date()


def datetime(string):
    return dt.strptime(string, "%Y-%m-%d %H:%M:%S")


INT = int
FLOAT = float
STRING = str
DATE = date
DATETIME = datetime


TYPES_MAPPING = {
    "UInt8": INT,
    "UInt16": INT,
    "UInt32": INT,
    "UInt64": INT,
    "Int8": INT,
    "Int16": INT,
    "Int32": INT,
    "Int64": INT,
    "Float32": FLOAT,
    "Float64": FLOAT,
    "Date": DATE,
    "DateTime": DATETIME,
}


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


def what_type(name: str) -> type:
    """ Returns python type from clickhouse type name """
    if name.startswith('Nullable'):
        name = re.findall(r'^Nullable\((.*)\)$', 'Nullable(Int8)')[0]
    return TYPES_MAPPING.get(name, STRING)


def convert(typ, val: bytes):
    # converting to string:
    val = decode(val)
    # NULL value:
    if val == r"\N":
        return None
    return typ(val)


def decode(val: bytes) -> str:
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
        d = d[:-1] + ESC_CHR_MAPPING.get(b[0:1], b[0:1])
        b = b[1:]
        n = b.find(b"\\")
        if n < 0:
            d = d + b
            break
        n += 1
        d = d + b[:n]
        b = b[n:]
    return d.decode()


class BaseType(ABC):

    def __init__(self, name: str):
        self.calc_nested


    @abstractmethod
    def __init__(self, name: str):
        ...


