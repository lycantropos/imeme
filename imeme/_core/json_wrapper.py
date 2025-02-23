import json
from typing import TypeAlias

from typing_extensions import Self

JsonObject: TypeAlias = dict[str, 'JsonValue']
JsonArray: TypeAlias = list['JsonValue']
JsonScalar: TypeAlias = bool | int | float | str | None
JsonValue: TypeAlias = JsonArray | JsonObject | JsonScalar


class JsonWrapper:
    _value: JsonValue

    __slots__ = ('_value',)

    def __new__(cls, value: JsonValue, /) -> Self:
        self = super().__new__(cls)
        self._value = value
        return self

    def __str__(self, /) -> str:
        return json.dumps(self._value)
