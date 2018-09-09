from typing import List
from collections import namedtuple
from aiochclient.types import what_type


__all__ = ["RecordsFabric", "NamedRecordsFabric"]


def prepare_line(line: bytes) -> List[str]:
    return line.decode().strip().split("\t")


class RecordsFabric:
    def __init__(self, tps: bytes):
        self.tps = [what_type(tp) for tp in prepare_line(tps)]
        self.record = tuple

    def new(self, vls: bytes):
        vls = prepare_line(vls)
        return self.record(typ(val) for typ, val in zip(self.tps, vls))


class NamedRecordsFabric(RecordsFabric):
    def __init__(self, names: bytes, tps: bytes):
        super().__init__(tps=tps)
        self.record = namedtuple("Record", prepare_line(names))
        self.record.__new__.__defaults__ = (None,) * len(self.record._fields)
