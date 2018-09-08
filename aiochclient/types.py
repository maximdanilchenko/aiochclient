from enum import Enum


__all__ = ["what_type"]


class Types(Enum):
    INT = int
    FLOAT = float
    STRING = str
    DATE = 4
    DATETIME = 5
    ENUM = 6
    LIST = 7
    TUPLE = 8
    NONE = 9


TYPES_MAPPING = {
    "UInt8": Types.INT,
    "UInt16": Types.INT,
    "UInt32": Types.INT,
    "UInt64": Types.INT,
    "Int8": Types.INT,
    "Int16": Types.INT,
    "Int32": Types.INT,
    "Int64": Types.INT,
    "Float32": Types.FLOAT,
    "Float64": Types.FLOAT,
    "String": Types.STRING,
    "FixedString": Types.STRING,
    "Date": Types.DATE,
    "DateTime": Types.DATE,
    "Enum8": Types.ENUM,
    "Enum16": Types.ENUM,
    "Array": Types.LIST,
    "Tuple": Types.TUPLE,
    "Nullable": Types.NONE,
}


def what_type(name: str) -> type:
    """ Should return python type from clickhouse type name """
    return TYPES_MAPPING[name].value
