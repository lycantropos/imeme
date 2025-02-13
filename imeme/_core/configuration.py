from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from pathlib import Path
from types import UnionType
from typing import Any, ClassVar, Generic, TypeVar, final, overload

import tomli
from typing_extensions import Self

_T = TypeVar('_T')
_ElementT = TypeVar('_ElementT')
_ValueT = TypeVar('_ValueT')


@final
class Configuration(Generic[_T]):
    @classmethod
    def from_toml_file_path(cls, file_path: Path, /) -> Self:
        return cls._from_raw_section(
            tomli.loads(file_path.read_text('utf-8')), file_path
        )

    def get_list(
        self: Configuration[Any | list[_ElementT]], key: str, /
    ) -> ConfigurationList[Any]:
        return self._content.get_list(key)

    def get_section(
        self: Configuration[Any | dict[str, _ValueT]], key: str, /
    ) -> ConfigurationSection[Any]:
        return self._content.get_subsection(key)

    @classmethod
    def _from_raw_section(
        cls, raw: dict[str, Any], file_path: Path, /
    ) -> Self:
        return cls(ConfigurationSection(raw, JsonPath('$'), file_path))

    def __init__(self, content: ConfigurationSection[_T], /) -> None:
        self._content = content

    def __repr__(self, /) -> str:
        return f'{type(self).__qualname__}({self._content!r})'


@final
class ConfigurationField(Generic[_T]):
    __slots__ = '_file_path', '_json_path', '_value'

    def __init__(
        self, value: _T, json_path: JsonPath, file_path: Path, /
    ) -> None:
        self._file_path, self._json_path, self._value = (
            file_path,
            json_path,
            value,
        )

    def __repr__(self, /) -> str:
        return (
            f'{type(self).__qualname__}'
            '('
            f'{self._value!r}, {self._json_path!r}, {self._file_path!r}'
            ')'
        )

    def __str__(self, /) -> str:
        return (
            f'{self._json_path} field of '
            f'{self._file_path.as_posix()} configuration file'
        )

    def extract_exact(self, type_: type[_T] | UnionType, /) -> _T:
        if not isinstance(self._value, type_):
            raise TypeError(
                f'{self} expected to be {type_}, but got {type(self._value)}.'
            )
        return self._value


@final
class ConfigurationSection(Mapping[str, ConfigurationField[_ValueT]]):
    def get_list(
        self: ConfigurationSection[Any | list[_ElementT]], key: str, /
    ) -> ConfigurationList[_ElementT]:
        return ConfigurationList(
            self[key].extract_exact(list),
            self._json_path.join_key(key),
            self._file_path,
        )

    def get_subsection(
        self: ConfigurationSection[Any | dict[str, _ValueT]], key: str, /
    ) -> ConfigurationSection[_ValueT]:
        return ConfigurationSection(
            self[key].extract_exact(dict),
            self._json_path.join_key(key),
            self._file_path,
        )

    def __init__(
        self, raw: dict[str, Any], json_path: JsonPath, file_path: Path, /
    ) -> None:
        self._file_path, self._json_path, self._raw = file_path, json_path, raw

    def __getitem__(self, key: str, /) -> ConfigurationField[_ValueT]:
        try:
            value = self._raw[key]
        except KeyError:
            raise KeyError(f'{self} does not contain "{key}" field.') from None
        else:
            return ConfigurationField(
                value, self._json_path.join_key(key), self._file_path
            )

    def __iter__(self, /) -> Iterator[str]:
        return iter(self._raw)

    def __len__(self, /) -> int:
        return len(self._raw)

    def __repr__(self, /) -> str:
        return (
            f'{type(self).__qualname__}'
            '('
            f'{self._raw!r}, {self._json_path!r}, {self._file_path!r}'
            ')'
        )

    def __str__(self, /) -> str:
        return (
            f'{self._json_path} section of '
            f'{self._file_path.as_posix()} configuration file'
        )


@final
class ConfigurationList(Sequence[ConfigurationField[_ElementT]]):
    def get_subsection(
        self: ConfigurationList[Any | dict[str, _ValueT]], index: int, /
    ) -> ConfigurationSection[Any]:
        return ConfigurationSection(
            self[index].extract_exact(dict),
            self._json_path.join_index(index),
            self._file_path,
        )

    @overload
    def __getitem__(self, index: int) -> ConfigurationField[_ElementT]: ...

    @overload
    def __getitem__(self, index: slice) -> Self: ...

    def __getitem__(
        self, index: int | slice
    ) -> ConfigurationField[_ElementT] | Self:
        if isinstance(index, slice):
            return type(self)(
                self._raw[index],
                self._json_path.join_slice(index),
                self._file_path,
            )
        try:
            value = self._raw[index]
        except IndexError:
            raise IndexError(
                f'{self} does not contain element with index {index}.'
            ) from None
        else:
            return ConfigurationField(
                value, self._json_path.join_index(index), self._file_path
            )

    def __init__(
        self, raw: list[_ElementT], json_path: JsonPath, file_path: Path, /
    ) -> None:
        self._file_path, self._json_path, self._raw = file_path, json_path, raw

    def __iter__(self, /) -> Iterator[ConfigurationField[_ElementT]]:
        return (
            ConfigurationField(
                element, self._json_path.join_index(index), self._file_path
            )
            for index, element in enumerate(self._raw)
        )

    def __len__(self, /) -> int:
        return len(self._raw)

    def __str__(self, /) -> str:
        return (
            f'"{self._json_path}" list of '
            f'{self._file_path.as_posix()} configuration file'
        )

    def __repr__(self, /) -> str:
        return (
            f'{type(self).__qualname__}'
            '('
            f'{self._raw!r}, {self._json_path!r}, {self._file_path!r}'
            ')'
        )


@final
class JsonPath:
    def join_index(self, index: int, /) -> Self:
        if not isinstance(index, self._index_type):
            raise TypeError(
                f'Expected index to be {self._index_type}, '
                f'but got {type(index)}.'
            )
        return type(self)(*self._components, f'[{index}]')

    def join_key(self, key: str, /) -> Self:
        if not isinstance(key, self._key_type):
            raise TypeError(
                f'Expected key to be {self._key_type}, but got {type(key)}.'
            )
        return type(self)(*self._components, f'.{key}')

    def join_slice(self, slice_: slice, /) -> Self:
        if not isinstance(slice_, self._slice_type):
            raise TypeError(
                f'Expected slice to be {self._slice_type}, '
                f'but got {type(slice_)}.'
            )
        return type(self)(
            *self._components, _slice_to_json_path_component(slice_)
        )

    _index_type: ClassVar[type[int]] = int
    _key_type: ClassVar[type[str]] = str
    _slice_type: ClassVar[type[slice]] = slice

    def __init__(self, *components: str) -> None:
        if not all(isinstance(component, str) for component in components):
            raise TypeError(
                f'All components of {type(self).__qualname__} must be {str}.'
            )
        if len(components) == 0:
            raise ValueError(
                f'{type(self).__qualname__} must contain '
                'at least one component.'
            )
        self._components = components

    def __str__(self, /) -> str:
        return ''.join(self._components)

    def __repr__(self, /) -> str:
        return (
            f'{type(self).__qualname__}'
            f'({", ".join(map(repr, self._components))})'
        )


def _slice_endpoint_to_string(value: int | None) -> str:
    return '' if value is None else str(value)


def _slice_to_json_path_component(slice_: slice, /) -> str:
    return (
        (
            '['
            f'{_slice_endpoint_to_string(slice_.start)}'
            ':'
            f'{_slice_endpoint_to_string(slice_.stop)}'
            ']'
        )
        if slice_.step is None
        else (
            '['
            f'{_slice_endpoint_to_string(slice_.start)}'
            ':'
            f'{_slice_endpoint_to_string(slice_.stop)}'
            ':'
            f'{slice_.step}'
            ']'
        )
    )
