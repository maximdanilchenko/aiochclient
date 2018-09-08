from typing import List
from collections import namedtuple
from aiochclient.types import what_type


__all__ = ["RecordFabric"]


def prepare_line(line: bytes) -> List[str]:
    return line.decode().strip().split("\t")


class RecordFabric:
    def __init__(self, names: bytes, tps: bytes):
        self.tps = [what_type(tp) for tp in prepare_line(tps)]
        self.record = namedtuple("Record", prepare_line(names))
        self.record.__new__.__defaults__ = (None,) * len(self.record._fields)

    def new(self, vls: bytes):
        return self.record(typ(val) for typ, val in zip(self.tps, prepare_line(vls)))
