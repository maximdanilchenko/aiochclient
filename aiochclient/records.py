from collections.abc import Mapping
from typing import Any, Callable, Dict, Iterator, List, Tuple, Union

from aiochclient.exceptions import ChClientError

# Optional cython extension:
try:
    from aiochclient._types import what_py_converter
except ImportError:
    from aiochclient.types import what_py_converter

__all__ = ["RecordsFabric", "Record"]


class Record(Mapping):

    __slots__ = ("_converters", "_decoded", "_names", "_row")

    def __init__(
        self, row: Tuple[bytes], names: Dict[str, Any], converters: List[Callable]
    ):
        self._row: Tuple[Any] = row
        if not self._row:
            # in case of empty row
            self._decoded = True
            self._converters = []
            self._names = {}
        else:
            if len(row) != len(names):
                raise ChClientError("Incorrect response from ClickHouse")
            self._decoded = False
            self._converters = converters
            self._names = names

    def __getitem__(self, key: Union[str, int, slice]) -> Any:
        if not self._decoded:
            self._decode()
        if type(key) == str:
            try:
                return self._row[self._names[key]]
            except KeyError:
                if not self._row:
                    raise KeyError("Empty row. May be it is result of 'WITH TOTALS' query.")
                raise KeyError(f"No fields with name '{key}'")
        try:
            return self._row[key]
        except IndexError:
            if not self._row:
                raise IndexError("Empty row. May be it is result of 'WITH TOTALS' query.")
            raise IndexError(f"No fields with index '{key}'")

    def __iter__(self) -> Iterator:
        return iter(self._names)

    def __len__(self) -> int:
        return len(self._row)

    def _decode(self):
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

    def new(self, vls: bytes) -> Record:
        vls = vls[:-1]
        return Record(
            row=tuple(vls.split(b"\t")) if vls else (),
            names=self.names,
            converters=self.converters,
        )
