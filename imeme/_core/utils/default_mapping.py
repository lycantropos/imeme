from collections.abc import Callable, Iterator, MutableMapping
from typing import TypeVar

_Key = TypeVar('_Key')
_Value = TypeVar('_Value')


class DefaultMapping(MutableMapping[_Key, _Value]):
    def __delitem__(self, key: _Key, /) -> None:
        del self._data[key]

    def __getitem__(self, key: _Key, /) -> _Value:
        try:
            return self._data[key]
        except KeyError:
            result = self._data[key] = self._constructor(key)
            return result

    def __init__(
        self,
        constructor: Callable[[_Key], _Value],
        data: dict[_Key, _Value] | None = None,
        /,
    ) -> None:
        self._constructor = constructor
        self._data = data or {}

    def __iter__(self, /) -> Iterator[_Key]:
        return iter(self._data)

    def __len__(self, /) -> int:
        return len(self._data)

    def __setitem__(self, key: _Key, value: _Value, /) -> None:
        self._data[key] = value
