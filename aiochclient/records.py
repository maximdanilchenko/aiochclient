# Optional cython extension:
try:
    from aiochclient._types import what_py_type
except ImportError:
    from aiochclient.types import what_py_type


__all__ = ["RecordsFabric"]


class RecordsFabric:

    __slots__ = ("tps",)

    def __init__(self, tps: bytes):
        self.tps = [what_py_type(tp) for tp in tps.decode().strip().split("\t")]

    def new(self, vls: bytes):
        vls = vls[:-1]
        if not vls:  # In case of empty row
            return ()
        return tuple(typ.convert(val) for typ, val in zip(self.tps, vls.split(b"\t")))
