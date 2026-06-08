#cython: language_level=3
import datetime as _dt
import json
import re
import struct
from decimal import Decimal
from ipaddress import IPv4Address, IPv6Address
from uuid import UUID
from zoneinfo import ZoneInfo

from cpython cimport PyList_Append, PyUnicode_AsEncodedString, PyUnicode_Join
from cpython.datetime cimport (
    date,
    date_new,
    datetime,
    datetime_new,
    import_datetime,
)
from cpython.unicode cimport PyUnicode_DecodeUTF8
from cpython.mem cimport PyMem_Free, PyMem_Malloc
from libc.stdint cimport (
    int8_t,
    int16_t,
    int32_t,
    int64_t,
    uint8_t,
    uint16_t,
    uint32_t,
    uint64_t,
)
from libc.string cimport memcpy

from aiochclient.exceptions import ChClientError, NeedMoreData

import_datetime()


cdef inline void _civil_from_days(long long z, int* y, int* m, int* d):
    # Howard Hinnant's algorithm: days since 1970-01-01 -> (year, month, day).
    z += 719468
    cdef long long era = (z if z >= 0 else z - 146096) // 146097
    cdef long long doe = z - era * 146097
    cdef long long yoe = (doe - doe // 1460 + doe // 36524 - doe // 146096) // 365
    cdef long long year = yoe + era * 400
    cdef long long doy = doe - (365 * yoe + yoe // 4 - yoe // 100)
    cdef long long mp = (5 * doy + 2) // 153
    cdef long long day = doy - (153 * mp + 2) // 5 + 1
    cdef long long month = mp + 3 if mp < 10 else mp - 9
    if month <= 2:
        year += 1
    y[0] = <int>year
    m[0] = <int>month
    d[0] = <int>day


cdef class Cursor:
    """Fast forward cursor over a bytes buffer (RowBinary engine)."""

    cdef:
        bytes buf
        public Py_ssize_t pos
        Py_ssize_t size

    def __init__(self, bytes buf=b"", Py_ssize_t pos=0):
        self.buf = buf
        self.pos = pos
        self.size = len(buf)

    cpdef bytes read(self, Py_ssize_t n):
        cdef Py_ssize_t end = self.pos + n
        if end > self.size:
            raise NeedMoreData()
        cdef bytes chunk = self.buf[self.pos:end]
        self.pos = end
        return chunk

    cpdef read_varint(self):
        cdef:
            const unsigned char* data = self.buf
            Py_ssize_t pos = self.pos
            unsigned long long result = 0
            int shift = 0
            unsigned char byte
        while True:
            if pos >= self.size:
                raise NeedMoreData()
            byte = data[pos]
            pos += 1
            result |= (<unsigned long long>(byte & 0x7F)) << shift
            if not (byte & 0x80):
                self.pos = pos
                return result
            shift += 7

    cpdef object read_int(self, int size, bint signed):
        # Wider than 64 bits (Int128/256, Decimal128/256) -> python big-int.
        if size > 8:
            return int.from_bytes(self.read(size), "little", signed=signed)
        cdef:
            const unsigned char* data
            Py_ssize_t pos = self.pos
            uint64_t u = 0
            int i
        if pos + size > self.size:
            raise NeedMoreData()
        data = self.buf
        for i in range(size):
            u |= (<uint64_t>data[pos + i]) << (8 * i)
        self.pos = pos + size
        if signed:
            if size < 8 and (u >> (size * 8 - 1)) & 1:
                u |= (<uint64_t>0xFFFFFFFFFFFFFFFF) << (size * 8)
            return <int64_t>u
        return u

    cpdef double read_double(self):
        cdef double value
        if self.pos + 8 > self.size:
            raise NeedMoreData()
        memcpy(&value, (<const char*>self.buf) + self.pos, 8)
        self.pos += 8
        return value

    cpdef double read_float(self):
        cdef float value
        if self.pos + 4 > self.size:
            raise NeedMoreData()
        memcpy(&value, (<const char*>self.buf) + self.pos, 4)
        self.pos += 4
        return value

    # -- Native engine: whole-column bulk reads (raise NeedMoreData on short) --

    cpdef list read_string_column(self, Py_ssize_t n):
        cdef:
            const unsigned char* data = self.buf
            Py_ssize_t pos = self.pos
            Py_ssize_t size = self.size
            list out = []
            Py_ssize_t i, length, shift, end
            unsigned char b
        for i in range(n):
            length = 0
            shift = 0
            while True:
                if pos >= size:
                    raise NeedMoreData()
                b = data[pos]
                pos += 1
                length |= (<Py_ssize_t>(b & 0x7F)) << shift
                if not (b & 0x80):
                    break
                shift += 7
            end = pos + length
            if end > size:
                raise NeedMoreData()
            out.append(PyUnicode_DecodeUTF8(<char*>data + pos, length, NULL))
            pos = end
        self.pos = pos
        return out

    cpdef list read_date_column(self, Py_ssize_t n):
        cdef:
            const unsigned char* data = self.buf
            Py_ssize_t pos = self.pos
            list out = []
            Py_ssize_t i
            unsigned int days
            int y, m, d
        if pos + n * 2 > self.size:
            raise NeedMoreData()
        for i in range(n):
            days = data[pos] | (data[pos + 1] << 8)
            pos += 2
            _civil_from_days(days, &y, &m, &d)
            out.append(date_new(y, m, d))
        self.pos = pos
        return out

    cpdef list read_datetime_column(self, Py_ssize_t n):
        cdef:
            const unsigned char* data = self.buf
            Py_ssize_t pos = self.pos
            list out = []
            Py_ssize_t i
            unsigned long long secs
            int rem, y, m, d, hh, mm, ss
        if pos + n * 4 > self.size:
            raise NeedMoreData()
        for i in range(n):
            secs = (
                data[pos]
                | (data[pos + 1] << 8)
                | (data[pos + 2] << 16)
                | (<unsigned long long>data[pos + 3] << 24)
            )
            pos += 4
            _civil_from_days(<long long>(secs // 86400), &y, &m, &d)
            rem = <int>(secs % 86400)
            hh = rem // 3600
            rem = rem % 3600
            mm = rem // 60
            ss = rem % 60
            out.append(datetime_new(y, m, d, hh, mm, ss, 0, None))
        self.pos = pos
        return out


# ---- RowBinary engine: base type with virtual read/write -------------------

RB_EPOCH_DATE = _dt.date(1970, 1, 1)
RB_EPOCH_DATETIME = _dt.datetime(1970, 1, 1)
RB_EPOCH_UTC = _dt.datetime(1970, 1, 1, tzinfo=_dt.timezone.utc)
_TZ_UNSET = object()


cdef bytes rb_write_varint(unsigned long long value):
    cdef bytearray out = bytearray()
    cdef unsigned char byte
    while True:
        byte = value & 0x7F
        value >>= 7
        if value:
            out.append(byte | 0x80)
        else:
            out.append(byte)
            return bytes(out)


cdef str rb_parse_tz(str name):
    cdef Py_ssize_t first, last
    if "'" in name:
        first = name.index("'")
        last = name.rindex("'")
        return name[first + 1:last].replace("\\", "")
    return None


cdef class _RBType:
    """Base for the RowBinary read/write virtual dispatch."""

    cdef object read(self, Cursor cursor):
        raise ChClientError(
            f"RowBinary decoding is not implemented for type '{type(self).__name__}'"
        )

    cpdef bytes write(self, value):
        raise ChClientError(
            f"RowBinary encoding is not implemented for type '{type(self).__name__}'"
        )


cpdef tuple read_row(Cursor cursor, tuple readers):
    """Read one RowBinary row given the column type objects (fast dispatch)."""
    cdef:
        _RBType reader
        list out = []
    for reader in readers:
        out.append(reader.read(cursor))
    return tuple(out)


cdef datetime _datetime_parse(str string):
    return datetime.strptime(string, '%Y-%m-%d %H:%M:%S')

cdef datetime _datetime_parse_f(str string):
    # ClickHouse DateTime64 may carry up to 9 fractional digits (nanoseconds),
    # but Python datetime only supports microseconds and strptime's "%f"
    # rejects more than 6 digits. Truncate the fractional part to microseconds
    # so DateTime64(7..9) parses — matching ciso8601 when it is installed.
    cdef:
        Py_ssize_t dot = string.find('.')
        datetime parsed
    if dot < 0:
        return datetime.strptime(string, '%Y-%m-%d %H:%M:%S')
    parsed = datetime.strptime(string[:dot], '%Y-%m-%d %H:%M:%S')
    return parsed.replace(microsecond=int(string[dot + 1:dot + 7].ljust(6, '0')))

cdef date _date_parse(str string):
    return datetime.strptime(string, '%Y-%m-%d')


try:
    import ciso8601
except ImportError:
    date_parse = _date_parse
    datetime_parse = _datetime_parse
    datetime_parse_f = _datetime_parse_f
else:
    date_parse = datetime_parse = datetime_parse_f = ciso8601.parse_datetime


cdef extern from *:
    ctypedef int int128 "__int128_t"
    ctypedef int uint128 "__uint128_t"


__all__ = ["what_py_converter", "rows2ch", "json2ch", "py2ch"]


DEF DQ = "'"
DEF CM = ","
DEF COLON = ':'
DEF ESCAPE_OP = '\\'
DEF TUP_OP = '('
DEF TUP_CLS = ')'
DEF ARR_OP = '['
DEF ARR_CLS = ']'

RE_TUPLE = re.compile(r"^Tuple\((.*)\)$")
RE_ARRAY = re.compile(r"^Array\((.*)\)$")
RE_NESTED = re.compile(r"^Nested\((.*)\)$")
RE_NULLABLE = re.compile(r"^Nullable\((.*)\)$")
RE_LOW_CARDINALITY = re.compile(r"^LowCardinality\((.*)\)$")
RE_MAP = re.compile(r"^Map\((.*)\)$")
RE_REPLACE_QUOTE = re.compile(r"(?<!\\)'")


cdef str remove_single_quotes(str string):
    if string[0] == string[-1] == "'":
        return string[1:-1]
    return string


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

    try:
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
        return result
    finally:
        PyMem_Free(c_value_buffer)


cdef list seq_parser(str raw):
    """
    Parse the body of tuples, arrays and maps into the top-level,
    comma-separated elements, keeping quoted strings and nested brackets
    intact, so structural characters (``,``, ``(``, ``)``, ``[``, ``]``)
    inside a quoted string are not treated as separators.
    """
    cdef:
        list res = [], cur = []
        Py_ssize_t depth = 0
        bint in_str = False, escape_char = False
    if not raw:
        return res
    for sym in raw:
        if in_str:
            PyList_Append(cur, sym)
            if escape_char:
                escape_char = False
            elif sym == ESCAPE_OP:
                escape_char = True
            elif sym == DQ:
                in_str = False
            continue
        if sym == DQ:
            in_str = True
        elif sym == ARR_OP or sym == TUP_OP:
            depth += 1
        elif sym == ARR_CLS or sym == TUP_CLS:
            depth -= 1
        elif sym == CM and depth == 0:
            PyList_Append(res, PyUnicode_Join("", cur))
            del cur[:]
            continue
        PyList_Append(cur, sym)
    PyList_Append(res, PyUnicode_Join("", cur))
    return res


cdef tuple _split_map_kv(str pair):
    """Split a ``key:value`` map entry at its first top-level colon."""
    cdef:
        Py_ssize_t i = 0
        bint in_str = False, escape_char = False
    for sym in pair:
        if in_str:
            if escape_char:
                escape_char = False
            elif sym == ESCAPE_OP:
                escape_char = True
            elif sym == DQ:
                in_str = False
        elif sym == DQ:
            in_str = True
        elif sym == COLON:
            return pair[:i], pair[i + 1:]
        i += 1
    return pair, ""


cdef class StrType(_RBType):

    cdef:
        str name
        bint container

    def __cinit__(self, str name, bint container):
        self.name = name
        self.container = container

    cdef str _convert(self, str string):
        string = decode(string.encode())
        if self.container:
            return remove_single_quotes(string)
        return string

    cpdef str p_type(self, str string):
        return self._convert(string)

    cpdef str convert(self, bytes value):
        # ``value`` is the raw, still backslash-escaped bytes from ClickHouse.
        # ``_convert`` does the (single) escape-decoding, so we only utf-8 decode
        # here — decoding twice would re-interpret escape sequences and, for
        # example, turn a literal ``\t`` into a tab or drop a trailing backslash.
        return self._convert(value.decode())

    cdef object read(self, Cursor cursor):
        cdef Py_ssize_t length
        if self.name[0] == "F":  # FixedString(N)
            length = int(self.name[12:-1])
        else:
            length = cursor.read_varint()
        return cursor.read(length).decode()

    cpdef bytes write(self, value):
        cdef bytes data = value.encode()
        cdef Py_ssize_t length
        if self.name[0] == "F":
            length = int(self.name[12:-1])
            return data[:length].ljust(length, b"\x00")
        return rb_write_varint(len(data)) + data


cdef class BoolType(_RBType):

    cdef:
        str name
        bint container

    def __cinit__(self, str name, bint container):
        self.name = name
        self.container = container

    cpdef bint p_type(self, str string):
        if string == "true":
            return True
        elif string == "false":
            return False
        else:
            raise ValueError("invalid boolean value: {!r}".format(string))

    cpdef bint convert(self, bytes value):
        return self.p_type(value.decode())

    cdef object read(self, Cursor cursor):
        return cursor.read_int(1, False) != 0

    cpdef bytes write(self, value):
        return b"\x01" if value else b"\x00"


cdef class Int8Type(_RBType):

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

    cdef object read(self, Cursor cursor):
        return cursor.read_int(1, True)

    cpdef bytes write(self, value):
        return int(value).to_bytes(1, "little", signed=True)


cdef class Int16Type(_RBType):

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

    cdef object read(self, Cursor cursor):
        return cursor.read_int(2, True)

    cpdef bytes write(self, value):
        return int(value).to_bytes(2, "little", signed=True)


cdef class Int32Type(_RBType):

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

    cdef object read(self, Cursor cursor):
        return cursor.read_int(4, True)

    cpdef bytes write(self, value):
        return int(value).to_bytes(4, "little", signed=True)


cdef class Int64Type(_RBType):

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

    cdef object read(self, Cursor cursor):
        return cursor.read_int(8, True)

    cpdef bytes write(self, value):
        return int(value).to_bytes(8, "little", signed=True)


cdef class Int128Type(_RBType):

    cdef:
        str name
        bint container

    def __cinit__(self, str name, bint container):
        self.name = name
        self.container = container

    cpdef int128 p_type(self, str string):
        return int(string)

    cpdef int128 convert(self, bytes value):
        return int(value)

    cdef object read(self, Cursor cursor):
        return cursor.read_int(16, True)

    cpdef bytes write(self, value):
        return int(value).to_bytes(16, "little", signed=True)


cdef class Int256Type(_RBType):

    cdef:
        str name
        bint container

    def __cinit__(self, str name, bint container):
        self.name = name
        self.container = container

    cpdef p_type(self, str string):
        return int(string)

    cpdef convert(self, bytes value):
        return int(value)

    cdef object read(self, Cursor cursor):
        return cursor.read_int(32, True)

    cpdef bytes write(self, value):
        return int(value).to_bytes(32, "little", signed=True)


cdef class UInt8Type(_RBType):

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

    cdef object read(self, Cursor cursor):
        return cursor.read_int(1, False)

    cpdef bytes write(self, value):
        return int(value).to_bytes(1, "little")


cdef class UInt16Type(_RBType):

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

    cdef object read(self, Cursor cursor):
        return cursor.read_int(2, False)

    cpdef bytes write(self, value):
        return int(value).to_bytes(2, "little")


cdef class UInt32Type(_RBType):

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

    cdef object read(self, Cursor cursor):
        return cursor.read_int(4, False)

    cpdef bytes write(self, value):
        return int(value).to_bytes(4, "little")


cdef class UInt64Type(_RBType):

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

    cdef object read(self, Cursor cursor):
        return cursor.read_int(8, False)

    cpdef bytes write(self, value):
        return int(value).to_bytes(8, "little")


cdef class UInt128Type(_RBType):

    cdef:
        str name
        bint container

    def __cinit__(self, str name, bint container):
        self.name = name
        self.container = container

    cpdef uint128 p_type(self, str string):
        return int(string)

    cpdef uint128 convert(self, bytes value):
        return int(value)

    cdef object read(self, Cursor cursor):
        return cursor.read_int(16, False)

    cpdef bytes write(self, value):
        return int(value).to_bytes(16, "little")


cdef class UInt256Type(_RBType):

    cdef:
        str name
        bint container

    def __cinit__(self, str name, bint container):
        self.name = name
        self.container = container

    cpdef p_type(self, str string):
        return int(string)

    cpdef convert(self, bytes value):
        return int(value)

    cdef object read(self, Cursor cursor):
        return cursor.read_int(32, False)

    cpdef bytes write(self, value):
        return int(value).to_bytes(32, "little")


cdef class FloatType(_RBType):

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

    cdef object read(self, Cursor cursor):
        if self.name == "Float64":
            return cursor.read_double()
        return cursor.read_float()

    cpdef bytes write(self, value):
        if self.name == "Float64":
            return struct.pack("<d", value)
        return struct.pack("<f", value)


cdef class DateType(_RBType):

    cdef:
        str name
        bint container

    def __cinit__(self, str name, bint container):
        self.name = name
        self.container = container

    cdef object _convert(self, str string):
        string = string.strip("'")
        try:
            return date_parse(string).date()
        except ValueError:
            # In case of 0000-00-00
            if string == "0000-00-00":
                return None
            raise

    cpdef object p_type(self, str string):
        return self._convert(string)

    cpdef object convert(self, bytes value):
        return self._convert(value.decode())

    cdef object read(self, Cursor cursor):
        return RB_EPOCH_DATE + _dt.timedelta(days=cursor.read_int(2, False))

    cpdef bytes write(self, value):
        return (value - RB_EPOCH_DATE).days.to_bytes(2, "little")


cdef class DateTimeType(_RBType):

    cdef:
        str name
        bint container
        str _tz_name
        object _tz

    def __cinit__(self, str name, bint container):
        self.name = name
        self.container = container
        self._tz_name = rb_parse_tz(name)
        self._tz = _TZ_UNSET

    cdef object _zone(self):
        if self._tz is _TZ_UNSET:
            self._tz = ZoneInfo(self._tz_name) if self._tz_name else None
        return self._tz

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

    cdef object read(self, Cursor cursor):
        seconds = cursor.read_int(4, False)
        zone = self._zone()
        if zone is None:
            return RB_EPOCH_DATETIME + _dt.timedelta(seconds=seconds)
        return _dt.datetime.fromtimestamp(seconds, zone).replace(tzinfo=None)

    cpdef bytes write(self, value):
        zone = self._zone()
        if zone is None:
            seconds = (value - RB_EPOCH_DATETIME) // _dt.timedelta(seconds=1)
        else:
            seconds = int(value.replace(tzinfo=zone).timestamp())
        return int(seconds).to_bytes(4, "little")


cdef class DateTime64Type(_RBType):

    cdef:
        str name
        bint container
        int _precision
        str _tz_name
        object _tz

    def __cinit__(self, str name, bint container):
        self.name = name
        self.container = container
        self._precision = int(name[name.index("(") + 1:].split(",")[0].split(")")[0])
        self._tz_name = rb_parse_tz(name)
        self._tz = _TZ_UNSET

    cdef object _zone(self):
        if self._tz is _TZ_UNSET:
            self._tz = ZoneInfo(self._tz_name) if self._tz_name else None
        return self._tz

    cdef object _convert(self, str string):
        string = string.strip("'")
        try:
            return datetime_parse_f(string)
        except ValueError:
            # In case of 0000-00-00 00:00:00.000
            if string == "0000-00-00 00:00:00.000":
                return None
            raise

    cpdef object p_type(self, str string):
        return self._convert(string)

    cpdef object convert(self, bytes value):
        return self._convert(value.decode())

    cdef object read(self, Cursor cursor):
        ticks = cursor.read_int(8, True)
        if self._precision <= 6:
            micros = ticks * 10 ** (6 - self._precision)
        else:
            micros = ticks // 10 ** (self._precision - 6)
        zone = self._zone()
        if zone is None:
            return RB_EPOCH_DATETIME + _dt.timedelta(microseconds=micros)
        return (RB_EPOCH_UTC + _dt.timedelta(microseconds=micros)).astimezone(
            zone
        ).replace(tzinfo=None)

    cpdef bytes write(self, value):
        zone = self._zone()
        if zone is None:
            micros = (value - RB_EPOCH_DATETIME) // _dt.timedelta(microseconds=1)
        else:
            micros = (value.replace(tzinfo=zone) - RB_EPOCH_UTC) // _dt.timedelta(
                microseconds=1
            )
        if self._precision <= 6:
            ticks = micros // 10 ** (6 - self._precision)
        else:
            ticks = micros * 10 ** (self._precision - 6)
        return int(ticks).to_bytes(8, "little", signed=True)


cdef class TupleType(_RBType):

    cdef:
        str name
        bint container
        tuple types

    def __cinit__(self, str name, bint container):
        self.name = name
        self.container = container
        cdef str tps = RE_TUPLE.findall(name)[0]
        self.types = tuple(
            what_py_type(tp.rpartition(" ")[2], container=True) for tp in tps.split(",")
        )

    cdef tuple _convert(self, str string):
        return tuple(
            tp.p_type(val)
            for tp, val in zip(self.types, seq_parser(string[1:-1]))
        )

    cpdef tuple p_type(self, str string):
        return self._convert(string)

    cpdef tuple convert(self, bytes value):
        return self._convert(value.decode())

    cdef object read(self, Cursor cursor):
        cdef:
            _RBType tp
            list out = []
        for tp in self.types:
            out.append(tp.read(cursor))
        return tuple(out)

    cpdef bytes write(self, value):
        cdef:
            _RBType tp
            list parts = []
            Py_ssize_t i = 0
        for tp in self.types:
            parts.append(tp.write(value[i]))
            i += 1
        return b"".join(parts)


cdef class MapType(_RBType):

    cdef:
        str name
        bint container
        key_type
        value_type

    def __cinit__(self, str name, bint container):
        self.name = name
        self.container = container
        tps = RE_MAP.findall(name)[0]
        comma_index = tps.index(",")
        self.key_type = what_py_type(tps[:comma_index], container=True)
        self.value_type = what_py_type(tps[comma_index + 1:], container=True)

    cdef object read(self, Cursor cursor):
        cdef:
            Py_ssize_t n = cursor.read_varint()
            Py_ssize_t i
            dict out = {}
            _RBType k = <_RBType>self.key_type
            _RBType v = <_RBType>self.value_type
        for i in range(n):
            key = k.read(cursor)
            out[key] = v.read(cursor)
        return out

    cpdef bytes write(self, value):
        cdef:
            _RBType k = <_RBType>self.key_type
            _RBType v = <_RBType>self.value_type
            list parts = [rb_write_varint(len(value))]
        for key in value:
            parts.append(k.write(key))
            parts.append(v.write(value[key]))
        return b"".join(parts)

    cdef dict _convert(self, str string):
        cdef:
            dict result = {}
            str pair, key, value
        for pair in seq_parser(string[1:-1]):
            key, value = _split_map_kv(pair)
            result[self.key_type.p_type(key)] = self.value_type.p_type(value)
        return result

    cpdef dict p_type(self, string):
        return self._convert(string)

    cpdef dict convert(self, bytes value):
        return self._convert(value.decode())


cdef class ArrayType(_RBType):

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

    cdef object read(self, Cursor cursor):
        cdef:
            Py_ssize_t n = cursor.read_varint()
            Py_ssize_t i
            list out = []
            _RBType element = <_RBType>self.type
        for i in range(n):
            out.append(element.read(cursor))
        return out

    cpdef bytes write(self, value):
        cdef:
            _RBType element = <_RBType>self.type
            list parts = [rb_write_varint(len(value))]
        for elem in value:
            parts.append(element.write(elem))
        return b"".join(parts)

    cdef list _convert(self, str string):
        return [self.type.p_type(val) for val in seq_parser(string[1:-1])]

    cpdef list p_type(self, str string):
        return self._convert(string)

    cpdef list convert(self, bytes value):
        return self.p_type(value.decode())


cdef class NestedType(_RBType):

    cdef:
        str name
        bint container
        tuple types

    def __cinit__(self, str name, bint container):
        self.name = name
        self.container = container
        self.types = tuple(
            what_py_type(i.split()[1], container=True)
            for i in RE_NESTED.findall(name)[0].split(',')
        )

    cdef object read(self, Cursor cursor):
        cdef:
            Py_ssize_t n = cursor.read_varint()
            Py_ssize_t i
            list out = []
            _RBType tp
        for i in range(n):
            row = []
            for tp in self.types:
                row.append(tp.read(cursor))
            out.append(tuple(row))
        return out

    cpdef bytes write(self, value):
        cdef:
            _RBType tp
            list parts = [rb_write_varint(len(value))]
            Py_ssize_t j
        for row in value:
            j = 0
            for tp in self.types:
                parts.append(tp.write(row[j]))
                j += 1
        return b"".join(parts)

    cdef list _convert(self, str string):
        return self.p_type(string)

    cpdef list p_type(self, str string):
        result = []
        for val in seq_parser(string[1:-1]):
            temp = []
            for tp, elem in zip(self.types, seq_parser(val.strip("()"))):
                temp.append(tp.p_type(elem))
            result.append(tuple(temp))
        return result
    
    cpdef list convert(self, bytes value):
        return self._convert(value.decode())

cdef class NullableType(_RBType):

    cdef:
        str name
        bint container
        type

    def __cinit__(self, str name, bint container):
        self.name = name
        self.container = container
        self.type = what_py_type(RE_NULLABLE.findall(name)[0], container)

    cdef object read(self, Cursor cursor):
        if cursor.read(1) != b"\x00":
            return None
        return (<_RBType>self.type).read(cursor)

    cpdef bytes write(self, value):
        if value is None:
            return b"\x01"
        return b"\x00" + (<_RBType>self.type).write(value)

    cdef _convert(self, str string):
        if string == r"\N" or string == "NULL":
            return None
        return self.type.p_type(string)

    cpdef object p_type(self, str string):
        return self._convert(string)

    cpdef object convert(self, bytes value):
        return self._convert(decode(value))


cdef class NothingType(_RBType):

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

    cdef object read(self, Cursor cursor):
        return None

    cpdef bytes write(self, value):
        return b""


cdef class UUIDType(_RBType):

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

    cdef object read(self, Cursor cursor):
        cdef bytes data = cursor.read(16)
        return UUID(
            int=(int.from_bytes(data[:8], "little") << 64)
            | int.from_bytes(data[8:], "little")
        )

    cpdef bytes write(self, value):
        if not isinstance(value, UUID):
            value = UUID(str(value))
        cdef object number = value.int
        return (number >> 64).to_bytes(8, "little") + (
            number & 0xFFFFFFFFFFFFFFFF
        ).to_bytes(8, "little")


cdef class IPv4Type(_RBType):

    cdef:
        str name
        bint container

    def __cinit__(self, str name, bint container):
        self.name = name
        self.container = container

    cdef object _convert(self, str string):
        return IPv4Address(string.strip("'"))

    cpdef object p_type(self, str string):
        return self._convert(string)

    cpdef object convert(self, bytes value):
        return self._convert(value.decode())

    cdef object read(self, Cursor cursor):
        return IPv4Address(cursor.read_int(4, False))

    cpdef bytes write(self, value):
        return int(IPv4Address(value)).to_bytes(4, "little")


cdef class IPv6Type(_RBType):

    cdef:
        str name
        bint container

    def __cinit__(self, str name, bint container):
        self.name = name
        self.container = container

    cdef object _convert(self, str string):
        return IPv6Address(string.strip("'"))

    cpdef object p_type(self, str string):
        return self._convert(string)

    cpdef object convert(self, bytes value):
        return self._convert(value.decode())

    cdef object read(self, Cursor cursor):
        return IPv6Address(cursor.read(16))

    cpdef bytes write(self, value):
        return IPv6Address(value).packed


cdef class LowCardinalityType(_RBType):

    cdef:
        str name
        bint container
        type

    def __cinit__(self, str name, bint container):
        self.name = name
        self.container = container
        self.type = what_py_type(RE_LOW_CARDINALITY.findall(name)[0], container)

    cdef _convert(self, str string):
        return self.type.p_type(string)

    cpdef object p_type(self, str string):
        return self._convert(string)

    cpdef object convert(self, bytes value):
        return self._convert(decode(value))

    cdef object read(self, Cursor cursor):
        return (<_RBType>self.type).read(cursor)

    cpdef bytes write(self, value):
        return (<_RBType>self.type).write(value)


cdef class DecimalType(_RBType):

    cdef:
        str name
        bint container
        int _size
        int _scale

    def __cinit__(self, str name, bint container):
        self.name = name
        self.container = container
        cdef list nums = [int(n) for n in re.findall(r"\d+", name)]
        cdef int precision
        if name.startswith("Decimal("):
            precision = nums[0]
            self._scale = nums[1]
        else:
            precision = {
                "Decimal32": 9,
                "Decimal64": 18,
                "Decimal128": 38,
                "Decimal256": 76,
            }[name.split("(")[0]]
            self._scale = nums[-1]
        self._size = (
            4 if precision <= 9 else 8 if precision <= 18 else 16 if precision <= 38 else 32
        )

    cpdef object p_type(self, str string):
        return Decimal(string)

    cpdef object convert(self, bytes value):
        return Decimal(value.decode())

    cdef object read(self, Cursor cursor):
        return Decimal(cursor.read_int(self._size, True)).scaleb(-self._scale)

    cpdef bytes write(self, value):
        return int(Decimal(value).scaleb(self._scale)).to_bytes(
            self._size, "little", signed=True
        )


cdef class EnumType(StrType):
    """Enum8/Enum16: TSV gives the label (handled by StrType); RowBinary gives
    the signed integer index, mapped back to its label here."""

    cdef:
        int _size
        dict _mapping
        dict _reverse

    def __cinit__(self, str name, bint container):
        self._size = 1 if name.startswith("Enum8") else 2
        self._mapping = {
            int(num): label
            for label, num in re.findall(r"'((?:[^'\\]|\\.)*)'\s*=\s*(-?\d+)", name)
        }
        self._reverse = {label: index for index, label in self._mapping.items()}

    cdef object read(self, Cursor cursor):
        return self._mapping[cursor.read_int(self._size, True)]

    cpdef bytes write(self, value):
        return self._reverse[value].to_bytes(self._size, "little", signed=True)


cdef dict CH_TYPES_MAPPING = {
    "Bool": BoolType,
    "UInt8": UInt8Type,
    "UInt16": UInt16Type,
    "UInt32": UInt32Type,
    "UInt64": UInt64Type,
    "UInt128": UInt128Type,
    "UInt256": UInt256Type,
    "Int8": Int8Type,
    "Int16": Int16Type,
    "Int32": Int32Type,
    "Int64": Int64Type,
    "Int128": Int128Type,
    "Int256": Int256Type,
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
    "Array": ArrayType,
    "Map": MapType,
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


cpdef what_py_type(str name, bint container = False):
    """ Returns needed type class from clickhouse type name """
    name = name.strip()
    try:
        if name.startswith('SimpleAggregateFunction') or name.startswith('AggregateFunction'):
            ch_type = re.findall(r',(.*)\)', name)[0].strip()
        else:
            ch_type = name.split("(")[0]
        return CH_TYPES_MAPPING[ch_type](name, container=container)
    except KeyError:
        raise ChClientError(f"Unrecognized type name: '{name}'")


cpdef what_py_converter(str name, bint container = False):
    """ Returns needed type class from clickhouse type name """
    return what_py_type(name, container).convert


cdef bytes unconvert_str(object value):
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
    return PyUnicode_AsEncodedString(PyUnicode_Join("", res), NULL, NULL)


cdef bytes unconvert_bool(object value):
    # We use an integer representation here to be compatible with older
    # ClickHouse versions that represent booleans as UInt8 values.
    return b"1" if value else b"0"


cdef bytes unconvert_int(object value):
    return b"%d" % value


cdef bytes unconvert_float(double value):
    return f"{value}".encode('latin-1')


cdef bytes unconvert_date(object value):
    return f"'{value}'".encode('latin-1')


cdef bytes unconvert_datetime(object value):
    return f"'{value}'".encode('latin-1')


cdef bytes unconvert_tuple(object value):
    return b"(" + b",".join(py2ch(elem) for elem in value) + b")"

cdef bytes unconvert_dict(object value):
    return (
        b"{" +
        b','.join(py2ch(key) + b':' + py2ch(val) for key, val in value.items()) +
        b"}"
    )

cdef bytes unconvert_array(object value):
    return b"[" + b",".join(py2ch(elem) for elem in value) + b"]"


cdef bytes unconvert_nullable(object value):
    return b"NULL"


cdef bytes unconvert_uuid(object value):
    return f"'{value}'".encode('latin-1')


cdef bytes unconvert_ipaddress(object value):
    return f"'{value}'".encode('latin-1')


cdef bytes unconvert_decimal(object value):
    return f'{value}'.encode('latin-1')


cdef dict PY_TYPES_MAPPING = {
    bool: unconvert_bool,
    int: unconvert_int,
    float: unconvert_float,
    str: unconvert_str,
    date: unconvert_date,
    datetime: unconvert_datetime,
    tuple: unconvert_tuple,
    dict: unconvert_dict,
    list: unconvert_array,
    type(None): unconvert_nullable,
    UUID: unconvert_uuid,
    Decimal: unconvert_decimal,
    IPv4Address: unconvert_ipaddress,
    IPv6Address: unconvert_ipaddress,
}


cpdef bytes py2ch(value):
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
            f"int, float, str, dt.date, dt.datetime, dict, tuple, list, uuid.UUID "
            f"(or a subclass of one of them, or None)."
        )
    return converter(value)

def rows2ch(*rows):
    return b",".join(unconvert_tuple(tuple(row)) for row in rows)


def json2ch(*records, dumps):
    return dumps(records)[1:-1]

def empty_convertor(bytes value):
    return value
