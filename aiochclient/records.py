from collections.abc import Mapping
from typing import Any, Callable, Dict, Iterator, List, Tuple, Union

from aiochclient.exceptions import ChClientError

# Optional cython extension:
try:
    from aiochclient._types import what_py_converter
except ImportError:
    from aiochclient.types import what_py_converter

__all__ = ["RecordsFabric"]


class Record(Mapping):

    __slots__ = ("_row", "_names", "_decoded", "_converters")

    def __init__(
        self, row: Tuple[bytes], names: Dict[str, Any], converters: List[Callable]
    ):
        if row and len(row) != len(names):
            raise ChClientError("Incorrect response from ClickHouse")
        self._row: Tuple[Any] = row
        self._decoded = False
        self._names = names
        self._converters = converters

    def __getitem__(self, key: Union[str, int, slice]) -> Any:
        if not self._decoded:
            self._decode()
        if type(key) == str:
            try:
                return self._row[self._names[key]]
            except KeyError:
                raise KeyError(f"No fields with this name: {key}")
        elif type(key) in (int, slice):
            try:
                return self._row[key]
            except IndexError:
                raise IndexError(f"No fields with this index: {key}")
        else:
            raise TypeError(f"Incorrect key/index type: {type(key)}")

    def __iter__(self) -> Iterator:
        if self._row:
            return iter(self._names)
        else:
            return iter(())

    def __len__(self) -> int:
        return len(self._row)

    def _decode(self):
        if self._row:
            self._row = tuple(typ(val) for typ, val in zip(self._converters, self._row))
        self._decoded = True


class RecordsFabric:

    __slots__ = ("converters", "names")

    def __init__(self, tps: bytes, names: bytes):
        names = names.decode().strip().split("\t")
        self.names = {key: index for (index, key) in enumerate(names)}
        self.converters = [
            what_py_converter(tp) for tp in tps.decode().strip().split("\t")
        ]

    def new(self, vls: bytes):
        vls = vls[:-1]
        return Record(
            row=tuple(vls.split(b"\t")) if vls else (),
            names=self.names,
            converters=self.converters,
        )
