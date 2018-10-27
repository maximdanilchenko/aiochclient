import re
from cpython cimport datetime as dt
from cpython cimport bool

from aiochclient.exceptions import ChClientError

__all__ = ["what_py_type", "rows2ch"]

cdef dict ESC_CHR_MAPPING = {
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

cdef str DQ = "'", CM = ","


cdef class BaseType:

    cdef str name
    cdef bool container

    __slots__ = ("name", "container")

    def __init__(self, str name, bool container):
        self.name = name
        self.container = container

    cdef p_type(self, str string):
        """ Function for implementing specific actions for each type """
        return string

    cdef str decode(self, bytes val):
        """
        Converting bytes from clickhouse with
        backslash-escaped special characters
        to pythonic string format
        """
        cdef int n = val.find(b"\\")
        cdef bytes d, b
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
            d += b[:n]
            b = b[n:]
        return d.decode()

    def seq_parser(self, str raw):
        """
        Generator for parsing tuples and arrays.
        Returns elements one by one
        """
        cdef list cur = []
        cdef str sym
        cdef int i, length = len(raw)
        cdef bool blocked = False
        if length == 0:
            return None
        for i in range(length):
            sym = raw[i]
            if sym == CM and not blocked:
                yield "".join(cur)
                cur = []
            elif sym == DQ:
                blocked = not blocked
                cur.append(sym)
            else:
                cur.append(sym)
        yield "".join(cur)

    cpdef convert(self, bytes value):
        return self.p_type(self.decode(value))

    @staticmethod
    def unconvert(str value):
        return f"'{value}'".encode()


cdef class StrType(BaseType):
    cdef p_type(self, str string):
        if self.container:
            return string.strip("'")
        return string

    @staticmethod
    def unconvert(str value):
        value = value.replace("\\", "\\\\").replace("'", "\\'")
        return f"'{value}'".encode()


cdef class IntType(BaseType):
    cdef p_type(self, str string):
        return int(string)

    @staticmethod
    def unconvert(int value):
        return f"{value}".encode()


cdef class FloatType(IntType):
    cdef p_type(self, str string):
        return float(string)

    @staticmethod
    def unconvert(float value):
        return f"{value}".encode()


cdef class DateType(BaseType):
    cdef p_type(self, str string):
        string = string.strip("'")
        try:
            return dt.datetime.strptime(string, "%Y-%m-%d").date()
        except ValueError:
            # In case of 0000-00-00
            if string == "0000-00-00":
                return None
            raise

    @staticmethod
    def unconvert(dt.date value):
        return f"'{value}'".encode()


cdef class DateTimeType(BaseType):
    cdef p_type(self, str string):
        string = string.strip("'")
        try:
            return dt.datetime.strptime(string, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            # In case of 0000-00-00 00:00:00
            if string == "0000-00-00 00:00:00":
                return None
            raise

    @staticmethod
    def unconvert(dt.datetime value):
        return f"'{value.replace(microsecond=0)}'".encode()


cdef class TupleType(BaseType):

    __slots__ = ("name", "types")

    cdef tuple types

    def __init__(self, str name, **kwargs):
        super().__init__(name, **kwargs)
        cdef str tps = re.findall(r"^Tuple\((.*)\)$", name)[0]
        self.types = tuple(what_py_type(tp, container=True) for tp in tps.split(","))

    def p_type(self, str string):
        return tuple(
            tp.p_type(val)
            for tp, val in zip(self.types, self.seq_parser(string.strip("()")))
        )

    @staticmethod
    def unconvert(tuple value):
        return b"(" + b",".join(py2ch(elem) for elem in value) + b")"


cdef class ArrayType(BaseType):

    __slots__ = ("name", "type")

    cdef BaseType type

    def __init__(self, str name, **kwargs):
        super().__init__(name, **kwargs)
        self.type = what_py_type(
            re.findall(r"^Array\((.*)\)$", name)[0], container=True
        )

    cdef p_type(self, str string):
        return [self.type.p_type(val) for val in self.seq_parser(string.strip("[]"))]

    @staticmethod
    def unconvert(list value):
        return b"[" + b",".join(py2ch(elem) for elem in value) + b"]"


cdef class NullableType(BaseType):

    __slots__ = ("name", "type")

    cdef BaseType type

    def __init__(self, str name, **kwargs):
        super().__init__(name, **kwargs)
        self.type = what_py_type(re.findall(r"^Nullable\((.*)\)$", name)[0])

    cdef p_type(self, str string):
        if string == r"\N" or string == "NULL":
            return None
        return self.type.p_type(string)

    @staticmethod
    def unconvert(value):
        return b"NULL"


cdef class NothingType(BaseType):
    cdef p_type(self, str string):
        return None


cdef dict CH_TYPES_MAPPING = {
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

cdef dict PY_TYPES_MAPPING = {
    int: IntType,
    float: FloatType,
    str: StrType,
    dt.date: DateType,
    dt.datetime: DateTimeType,
    tuple: TupleType,
    list: ArrayType,
    type(None): NullableType,
}


def what_py_type(str name, bool container = False):
    """ Returns needed type class from clickhouse type name """
    name = name.strip()
    try:
        return CH_TYPES_MAPPING[name.split("(")[0]](name, container=container)
    except KeyError:
        raise ChClientError(f"Unrecognized type name: '{name}'")


cdef py2ch(value):
    return what_ch_type(type(value)).unconvert(value)


def rows2ch(*rows):
    return b",".join(TupleType.unconvert(row) for row in rows)


cdef what_ch_type(typ):
    try:
        return PY_TYPES_MAPPING[typ]
    except KeyError:
        raise ChClientError(
            f"Unrecognized type: '{typ}'. "
            f"The value type should be exactly one of "
            f"int, float, str, dt.date, dt.datetime, tuple, list (or None). "
            f"No subclasses yet."
        )
