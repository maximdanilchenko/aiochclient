from aiochclient.types import what_type


__all__ = ["RecordsFabric"]


class RecordsFabric:
    def __init__(self, tps: bytes):
        self.tps = [what_type(tp) for tp in tps.decode().strip().split("\t")]
        self.record = tuple

    def new(self, vls: bytes):
        vls = vls[:-1]
        if not vls:  # In case of empty row
            return self.record()
        return self.record(
            typ.convert(val) for typ, val in zip(self.tps, vls.split(b"\t"))
        )
