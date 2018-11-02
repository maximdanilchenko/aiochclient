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

    def __cinit__(self, str name, bool container):
        self.name = name
        self.container = container

    cpdef p_type(self, str string):
        """ Function for implementing specific actions for each type """
        return string

    cdef str _decode(self, bytes val):
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

    cdef int find_index(self, unsigned char[:] val, char elem):
        cdef int i
        for i in range(val.shape[0]):
            if val[i] == elem:
                return i
        return -1

    cdef str decode(self, const unsigned char[:] sval):
        # print('started')
        cdef int dlen, blen, i
        cdef unsigned char[:] d
        cdef unsigned char[:] b, val
        cdef bytes py_string
        val = sval.copy()
        cdef int n = self.find_index(val, b"\\")
        if n < 0:
            py_string = bytes(val)
            return py_string.decode()
        n += 1
        d = val[:n]
        b = val[n:]
        while b.shape[0] > 0:
            if b[0:1] == b"b":
                d[n] =  b"\b"
            # elif b[0:1] == b"N":
            #     d[n] =  b"\N"
            elif b[0:1] == b"f":
                d[n] = b"\f"
            elif b[0:1] == b"r":
                d[n] = b"\r"
            elif b[0:1] == b"n":
                d[n] = b"\n"
            elif b[0:1] == b"t":
                d[n] = b"\t"
            elif b[0:1] == b"0":
                d[n] = b" "
            elif b[0:1] == b"'":
                d[n] = b"'"
            elif b[0:1] == b"\\":
                d[n] = b"\\"
            else:
                d[n] = b[0]
            b = b[1:]
            n = self.find_index(b, b"\\")
            if n < 0:
                dlen = d.shape[0]
                blen = b.shape[0]
                for i in range(blen):
                    d[dlen+i] = b[i]
                break
            n += 1
            dlen = d.shape[0]
            for i in range(n):
                d[dlen+i] = b[i]
            b = b[n:]
        # print("ended")
        py_string = bytes(d)
        return py_string.decode()


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
    cpdef p_type(self, str string):
        if self.container:
            return string.strip("'")
        return string

    @staticmethod
    def unconvert(str value):
        value = value.replace("\\", "\\\\").replace("'", "\\'")
        return f"'{value}'".encode()


cdef class IntType(BaseType):
    cpdef p_type(self, str string):
        return int(string)

    @staticmethod
    def unconvert(value):
        return f"{value}".encode()


cdef class FloatType(IntType):
    cpdef p_type(self, str string):
        return float(string)

    @staticmethod
    def unconvert(double value):
        return f"{value}".encode()


cdef class DateType(BaseType):
    cpdef p_type(self, str string):
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
    cpdef p_type(self, str string):
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

    cdef tuple types

    def __cinit__(self, str name, bool container):
        self.name = name
        self.container = container
        cdef str tps = re.findall(r"^Tuple\((.*)\)$", name)[0]
        self.types = tuple(what_py_type(tp, container=True) for tp in tps.split(","))

    def p_type(self, str string):
        return tuple(
            tp.p_type(val)
            for tp, val in zip(self.types, self.seq_parser(string.strip("()")))
        )

    cpdef convert(self, bytes value):
        return self.p_type(self.decode(value))

    @staticmethod
    def unconvert(tuple value):
        return b"(" + b",".join(py2ch(elem) for elem in value) + b")"


cdef class ArrayType(BaseType):

    cdef BaseType type

    def __cinit__(self, str name, bool container):
        self.name = name
        self.container = container
        self.type = what_py_type(
            re.findall(r"^Array\((.*)\)$", name)[0], container=True
        )

    cpdef p_type(self, str string):
        return [self.type.p_type(val) for val in self.seq_parser(string.strip("[]"))]

    @staticmethod
    def unconvert(list value):
        return b"[" + b",".join(py2ch(elem) for elem in value) + b"]"


cdef class NullableType(BaseType):

    cdef BaseType type

    def __cinit__(self, str name, bool container):
        self.name = name
        self.container = container
        self.type = what_py_type(re.findall(r"^Nullable\((.*)\)$", name)[0])

    cpdef p_type(self, str string):
        if string == r"\N" or string == "NULL":
            return None
        return self.type.p_type(string)

    @staticmethod
    def unconvert(value):
        return b"NULL"


cdef class NothingType(BaseType):
    cpdef p_type(self, str string):
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
