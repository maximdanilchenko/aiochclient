#cython: language_level=3
import re
from uuid import UUID
from cpython.datetime cimport date, datetime
from cpython cimport PyUnicode_Join, PyUnicode_AsEncodedString, PyList_Append
from cpython.mem cimport PyMem_Malloc, PyMem_Free
from libc.stdint cimport (int8_t, int16_t, int32_t, int64_t,
                          uint8_t, uint16_t, uint32_t, uint64_t)

from aiochclient.exceptions import ChClientError

try:
    import ciso8601

    datetime_parse = ciso8601.parse_datetime
except ImportError:
    from datetime import datetime

    datetime_parse = datetime.fromisoformat


__all__ = ["what_py_converter", "rows2ch"]


DEF DQ = "'"
DEF CM = ","

RE_TUPLE = re.compile(r"^Tuple\((.*)\)$")
RE_ARRAY = re.compile(r"^Array\((.*)\)$")
RE_NULLABLE = re.compile(r"^Nullable\((.*)\)$")
RE_LOW_CARDINALITY = re.compile(r"^LowCardinality\((.*)\)$")


cdef str decode(char* val):
    """
    Converting bytes from clickhouse with
    backslash-escaped special characters
    to pythonic string format
    """
    cdef:
        int current_chr
        str result
        Py_ssize_t i, current_i = 0, length = len(val)
        char* c_value_buffer = <char *> PyMem_Malloc(length * sizeof(char))
        bint escape = False

    for i in range(length):
        current_chr = val[i]
        if escape:
            # cython efficiently replaces it with switch/case
            if current_chr == ord("b"):
                c_value_buffer[current_i] = ord("\b")
            elif current_chr == ord("N"):
                c_value_buffer[current_i] = ord("\\")
                current_i += 1
                c_value_buffer[current_i] = ord("N")
            elif current_chr == ord("f"):
                c_value_buffer[current_i] = ord("\f")
            elif current_chr == ord("r"):
                c_value_buffer[current_i] = ord("\r")
            elif current_chr == ord("n"):
                c_value_buffer[current_i] = ord("\n")
            elif current_chr == ord("t"):
                c_value_buffer[current_i] = ord("\t")
            elif current_chr == ord("0"):
                c_value_buffer[current_i] = ord(" ")
            elif current_chr == ord("'"):
                c_value_buffer[current_i] = ord("'")
            elif current_chr == ord("\\"):
                c_value_buffer[current_i] = ord("\\")
            else:
                c_value_buffer[current_i] = current_chr
            escape = False
            current_i += 1
        elif current_chr == ord("\\"):
            escape = True
        else:
            c_value_buffer[current_i] = current_chr
            current_i += 1
    result = c_value_buffer[:current_i].decode()
    PyMem_Free(c_value_buffer)
    return result


cdef list seq_parser(str raw):
    """
    Function for parsing tuples and arrays
    """
    cdef:
        list res = [], cur = []
        bint blocked = False
    if not raw:
        return res
    for sym in raw:
        if sym == CM and not blocked:
            PyList_Append(res, PyUnicode_Join("", cur))
            del cur[:]
        elif sym == DQ:
            blocked = not blocked
            PyList_Append(cur, sym)
        else:
            PyList_Append(cur, sym)
    PyList_Append(res, PyUnicode_Join("", cur))
    return res


cdef class StrType:
    
    cdef:
        str name
        bint container

    def __cinit__(self, str name, bint container):
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
        bint container

    def __cinit__(self, str name, bint container):
        self.name = name
        self.container = container

    cpdef int8_t p_type(self, str string):
        return int(string)

    cpdef int8_t convert(self, bytes value):
        return int(value)


cdef class Int16Type:

    cdef:
        str name
        bint container

    def __cinit__(self, str name, bint container):
        self.name = name
        self.container = container

    cpdef int16_t p_type(self, str string):
        return int(string)

    cpdef int16_t convert(self, bytes value):
        return int(value)


cdef class Int32Type:

    cdef:
        str name
        bint container

    def __cinit__(self, str name, bint container):
        self.name = name
        self.container = container

    cpdef int32_t p_type(self, str string):
        return int(string)

    cpdef int32_t convert(self, bytes value):
        return int(value)


cdef class Int64Type:

    cdef:
        str name
        bint container

    def __cinit__(self, str name, bint container):
        self.name = name
        self.container = container

    cpdef int64_t p_type(self, str string):
        return int(string)

    cpdef int64_t convert(self, bytes value):
        return int(value)


cdef class UInt8Type:

    cdef:
        str name
        bint container

    def __cinit__(self, str name, bint container):
        self.name = name
        self.container = container

    cpdef uint8_t p_type(self, str string):
        return int(string)

    cpdef uint8_t convert(self, bytes value):
        return int(value)


cdef class UInt16Type:

    cdef:
        str name
        bint container

    def __cinit__(self, str name, bint container):
        self.name = name
        self.container = container

    cpdef uint16_t p_type(self, str string):
        return int(string)

    cpdef uint16_t convert(self, bytes value):
        return int(value)


cdef class UInt32Type:

    cdef:
        str name
        bint container

    def __cinit__(self, str name, bint container):
        self.name = name
        self.container = container

    cpdef uint32_t p_type(self, str string):
        return int(string)

    cpdef uint32_t convert(self, bytes value):
        return int(value)


cdef class UInt64Type:

    cdef:
        str name
        bint container

    def __cinit__(self, str name, bint container):
        self.name = name
        self.container = container

    cpdef uint64_t p_type(self, str string):
        return int(string)

    cpdef uint64_t convert(self, bytes value):
        return int(value)


cdef class FloatType:

    cdef:
        str name
        bint container

    def __cinit__(self, str name, bint container):
        self.name = name
        self.container = container

    cpdef double p_type(self, str string):
        return float(string)

    cpdef double convert(self, bytes value):
        return float(value)


cdef class DateType:

    cdef:
        str name
        bint container

    def __cinit__(self, str name, bint container):
        self.name = name
        self.container = container

    cdef object _convert(self, str string):
        string = string.strip("'")
        try:
            return datetime_parse(string).date()
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
        bint container

    def __cinit__(self, str name, bint container):
        self.name = name
        self.container = container

    cdef object _convert(self, str string):
        string = string.strip("'")
        try:
            return datetime_parse(string)
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
        bint container
        tuple types

    def __cinit__(self, str name, bint container):
        self.name = name
        self.container = container
        cdef str tps = RE_TUPLE.findall(name)[0]
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
        bint container
        type

    def __cinit__(self, str name, bint container):
        self.name = name
        self.container = container
        self.type = what_py_type(
            RE_ARRAY.findall(name)[0], container=True
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
        bint container
        type

    def __cinit__(self, str name, bint container):
        self.name = name
        self.container = container
        self.type = what_py_type(RE_NULLABLE.findall(name)[0])

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
        bint container

    def __cinit__(self, str name, bint container):
        self.name = name
        self.container = container

    cpdef void p_type(self, str string):
        pass
    
    cpdef void convert(self, bytes value):
        pass


cdef class UUIDType:

    cdef:
        str name
        bint container

    def __cinit__(self, str name, bint container):
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
        bint container
        type

    def __cinit__(self, str name, bint container):
        self.name = name
        self.container = container
        self.type = what_py_type(RE_LOW_CARDINALITY.findall(name)[0])

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


cdef what_py_type(str name, bint container = False):
    """ Returns needed type class from clickhouse type name """
    name = name.strip()
    try:
        return CH_TYPES_MAPPING[name.split("(")[0]](name, container=container)
    except KeyError:
        raise ChClientError(f"Unrecognized type name: '{name}'")


cpdef what_py_converter(str name, bint container = False):
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
    date: unconvert_date,
    datetime: unconvert_datetime,
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
