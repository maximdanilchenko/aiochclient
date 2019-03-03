import re
from uuid import UUID

from cpython cimport datetime as dt
from cpython cimport bool
from cpython cimport PyUnicode_Join, PyUnicode_AsEncodedString
from libc.stdint cimport (int8_t, int16_t, int32_t, int64_t,
                          uint8_t, uint16_t, uint32_t, uint64_t)

from aiochclient.exceptions import ChClientError

__all__ = ["what_py_converter", "rows2ch"]


cdef:
    dict ESC_CHR_MAPPING = {
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
DEF DQ = "'"
DEF CM = ","
DEF SL = b"\\"


cdef str decode(bytes val):
    """
    Converting bytes from clickhouse with
    backslash-escaped special characters
    to pythonic string format
    """
    cdef:
        int n = val.find(SL)
        bytes d, b
    if n < 0:
        return val.decode()
    n += 1
    d = val[:n]
    b = val[n:]
    while b:
        d = d[:-1] + ESC_CHR_MAPPING.get(b[0:1], b[0:1])
        b = b[1:]
        n = b.find(SL)
        if n < 0:
            d = d + b
            break
        n += 1
        d += b[:n]
        b = b[n:]
    return d.decode()


cdef list seq_parser(str raw):
    """
    Generator for parsing tuples and arrays.
    Returns elements one by one
    """
    cdef:
        list res = [], cur = []
        str sym
        int i, length = len(raw)
        bool blocked = False
    if length == 0:
        return res
    for i in range(length):
        sym = raw[i]
        if sym == CM and not blocked:
            res.append(PyUnicode_Join("", cur))
            del cur[:]
        elif sym == DQ:
            blocked = not blocked
            cur.append(sym)
        else:
            cur.append(sym)
    res.append(PyUnicode_Join("", cur))
    return res


cdef class StrType:
    
    cdef:
        str name
        bool container

    def __cinit__(self, str name, bool container):
        self.name = name
        self.container = container

    cdef str _convert(self, str string):
        if self.container:
            return string.strip("'")
        return string
    
    cpdef str p_type(self, str string):
        return self._convert(string)

    cpdef str convert(self, bytes value):
        return self._convert(decode(value))


cdef class Int8Type:

    cdef:
        str name
        bool container

    def __cinit__(self, str name, bool container):
        self.name = name
        self.container = container

    cpdef int8_t p_type(self, str string):
        return int(string)

    cpdef int8_t convert(self, bytes value):
        return int(value)


cdef class Int16Type:

    cdef:
        str name
        bool container

    def __cinit__(self, str name, bool container):
        self.name = name
        self.container = container

    cpdef int16_t p_type(self, str string):
        return int(string)

    cpdef int16_t convert(self, bytes value):
        return int(value)


cdef class Int32Type:

    cdef:
        str name
        bool container

    def __cinit__(self, str name, bool container):
        self.name = name
        self.container = container

    cpdef int32_t p_type(self, str string):
        return int(string)

    cpdef int32_t convert(self, bytes value):
        return int(value)


cdef class Int64Type:

    cdef:
        str name
        bool container

    def __cinit__(self, str name, bool container):
        self.name = name
        self.container = container

    cpdef int64_t p_type(self, str string):
        return int(string)

    cpdef int64_t convert(self, bytes value):
        return int(value)


cdef class UInt8Type:

    cdef:
        str name
        bool container

    def __cinit__(self, str name, bool container):
        self.name = name
        self.container = container

    cpdef uint8_t p_type(self, str string):
        return int(string)

    cpdef uint8_t convert(self, bytes value):
        return int(value)


cdef class UInt16Type:

    cdef:
        str name
        bool container

    def __cinit__(self, str name, bool container):
        self.name = name
        self.container = container

    cpdef uint16_t p_type(self, str string):
        return int(string)

    cpdef uint16_t convert(self, bytes value):
        return int(value)


cdef class UInt32Type:

    cdef:
        str name
        bool container

    def __cinit__(self, str name, bool container):
        self.name = name
        self.container = container

    cpdef uint32_t p_type(self, str string):
        return int(string)

    cpdef uint32_t convert(self, bytes value):
        return int(value)


cdef class UInt64Type:

    cdef:
        str name
        bool container

    def __cinit__(self, str name, bool container):
        self.name = name
        self.container = container

    cpdef uint64_t p_type(self, str string):
        return int(string)

    cpdef uint64_t convert(self, bytes value):
        return int(value)


cdef class FloatType:

    cdef:
        str name
        bool container

    def __cinit__(self, str name, bool container):
        self.name = name
        self.container = container

    cpdef double p_type(self, str string):
        return float(string)

    cpdef double convert(self, bytes value):
        return float(value)


cdef class DateType:

    cdef:
        str name
        bool container

    def __cinit__(self, str name, bool container):
        self.name = name
        self.container = container

    cdef object _convert(self, str string):
        string = string.strip("'")
        try:
            return dt.datetime.strptime(string, "%Y-%m-%d").date()
        except ValueError:
            # In case of 0000-00-00
            if string == "0000-00-00":
                return None
            raise

    cpdef object p_type(self, str string):
        return self._convert(string)

    cpdef object convert(self, bytes value):
        return self._convert(value.decode())


cdef class DateTimeType:

    cdef:
        str name
        bool container

    def __cinit__(self, str name, bool container):
        self.name = name
        self.container = container

    cdef object _convert(self, str string):
        string = string.strip("'")
        try:
            return dt.datetime.strptime(string, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            # In case of 0000-00-00 00:00:00
            if string == "0000-00-00 00:00:00":
                return None
            raise

    cpdef object p_type(self, str string):
        return self._convert(string)

    cpdef object convert(self, bytes value):
        return self._convert(value.decode())


cdef class TupleType:

    cdef:
        str name
        bool container
        tuple types

    def __cinit__(self, str name, bool container):
        self.name = name
        self.container = container
        cdef str tps = re.findall(r"^Tuple\((.*)\)$", name)[0]
        self.types = tuple(what_py_type(tp, container=True).p_type for tp in tps.split(","))

    cdef tuple _convert(self, str string):
        return tuple(
            tp(val)
            for tp, val in zip(self.types, seq_parser(string.strip("()")))
        )

    cpdef tuple p_type(self, str string):
        return self._convert(string)

    cpdef tuple convert(self, bytes value):
        return self._convert(decode(value))


cdef class ArrayType:

    cdef:
        str name
        bool container
        type

    def __cinit__(self, str name, bool container):
        self.name = name
        self.container = container
        self.type = what_py_type(
            re.findall(r"^Array\((.*)\)$", name)[0], container=True
        )

    cdef list _convert(self, str string):
        return [self.type.p_type(val) for val in seq_parser(string.strip("[]"))]

    cpdef list p_type(self, str string):
        return self._convert(string)

    cpdef list convert(self, bytes value):
        return self.p_type(decode(value))


cdef class NullableType:

    cdef:
        str name
        bool container
        type

    def __cinit__(self, str name, bool container):
        self.name = name
        self.container = container
        self.type = what_py_type(re.findall(r"^Nullable\((.*)\)$", name)[0])

    cdef _convert(self, str string):
        if string == r"\N" or string == "NULL":
            return None
        return self.type.p_type(string)

    cpdef object p_type(self, str string):
        return self._convert(string)

    cpdef object convert(self, bytes value):
        return self._convert(decode(value))


cdef class NothingType:

    cdef:
        str name
        bool container

    def __cinit__(self, str name, bool container):
        self.name = name
        self.container = container

    cpdef void p_type(self, str string):
        pass
    
    cpdef void convert(self, bytes value):
        pass


cdef class UUIDType:

    cdef:
        str name
        bool container

    def __cinit__(self, str name, bool container):
        self.name = name
        self.container = container

    cdef object _convert(self, str string):
        return UUID(string.strip("'"))

    cpdef object p_type(self, str string):
        return self._convert(string)

    cpdef object convert(self, bytes value):
        return self._convert(value.decode())


cdef class LowCardinalityType:

    cdef:
        str name
        bool container
        type

    def __cinit__(self, str name, bool container):
        self.name = name
        self.container = container
        self.type = what_py_type(re.findall(r"^LowCardinality\((.*)\)$", name)[0])

    cdef _convert(self, str string):
        return self.type.p_type(string)

    cpdef object p_type(self, str string):
        return self._convert(string)

    cpdef object convert(self, bytes value):
        return self._convert(decode(value))


cdef dict CH_TYPES_MAPPING = {
    "UInt8": UInt8Type,
    "UInt16": UInt16Type,
    "UInt32": UInt32Type,
    "UInt64": UInt64Type,
    "Int8": Int8Type,
    "Int16": Int16Type,
    "Int32": Int32Type,
    "Int64": Int64Type,
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
    "UUID": UUIDType,
    "LowCardinality": LowCardinalityType,
}


cdef what_py_type(str name, bool container = False):
    """ Returns needed type class from clickhouse type name """
    name = name.strip()
    try:
        return CH_TYPES_MAPPING[name.split("(")[0]](name, container=container)
    except KeyError:
        raise ChClientError(f"Unrecognized type name: '{name}'")


cpdef what_py_converter(str name, bool container = False):
    """ Returns needed type class from clickhouse type name """
    return what_py_type(name, container).convert


cdef bytes unconvert_str(str value):
    cdef:
        list res = ["'"]
        int i, sl = len(value)
    for i in range(sl):
        if value[i] == "\\":
            res.append("\\\\")
        elif value[i] == "'":
            res.append("\\'")
        else:
            res.append(value[i])
    res.append("'")
    # return ''.join(res).encode()
    return PyUnicode_AsEncodedString(PyUnicode_Join("", res), NULL, NULL)


cdef bytes unconvert_int(object value):
    return b"%d" % value


cdef bytes unconvert_float(double value):
    # return f"{value}".encode()
    return PyUnicode_AsEncodedString(str(value), NULL, NULL)


cdef bytes unconvert_date(object value):
    # return f"'{value}'".encode()
    return PyUnicode_AsEncodedString(f"'{value}'", NULL, NULL)


cdef bytes unconvert_datetime(object value):
    # return f"'{value.replace(microsecond=0)}'".encode()
    return PyUnicode_AsEncodedString(f"'{value.replace(microsecond=0)}'", NULL, NULL)


cdef bytes unconvert_tuple(tuple value):
    return b"(" + b",".join(py2ch(elem) for elem in value) + b")"


cdef bytes unconvert_array(list value):
    return b"[" + b",".join(py2ch(elem) for elem in value) + b"]"


cdef bytes unconvert_nullable(object value):
    return b"NULL"


cdef bytes unconvert_uuid(object value):
    return f"'{value}'".encode()


cdef dict PY_TYPES_MAPPING = {
    int: unconvert_int,
    float: unconvert_float,
    str: unconvert_str,
    dt.date: unconvert_date,
    dt.datetime: unconvert_datetime,
    tuple: unconvert_tuple,
    list: unconvert_array,
    type(None): unconvert_nullable,
    UUID: unconvert_uuid,
}


cdef bytes py2ch(value):
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
    return b",".join(unconvert_tuple(tuple(row)) for row in rows)
