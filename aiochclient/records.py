# Optional cython extension:
try:
    from aiochclient._types import what_py_type
except ImportError:
    from aiochclient.types import what_py_type


__all__ = ["RecordsFabric"]


class RecordsFabric:

    __slots__ = ("converters",)

    def __init__(self, tps: bytes):
        self.converters = [what_py_type(tp).convert for tp in tps.decode().strip().split("\t")]

    def new(self, vls: bytes):
        vls = vls[:-1]
        if not vls:  # In case of empty row
            return ()
        # return tuple(typ(val) for typ, val in zip(self.converters, splitter(vls)))
        return tuple(typ(val) for typ, val in zip(self.converters, vls.split(b"\t")))
