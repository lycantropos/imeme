from __future__ import annotations

from typing import TypeAlias

from telethon import TelegramClient  # type: ignore[import-untyped]
from telethon.tl.types import (  # type: ignore[import-untyped]
    Channel,
    Chat,
    TypeInputPeer,
    User,
)
from telethon.utils import (  # type: ignore[import-untyped]
    get_input_peer,
    get_peer_id,
)
from typing_extensions import Self

PeerEntity: TypeAlias = User | Channel | Chat


class Peer:
    @classmethod
    async def from_raw(
        cls, raw: RawPeer, /, *, client: TelegramClient
    ) -> Self:
        entity = await client.get_entity(raw)
        if not isinstance(entity, PeerEntity):
            raise TypeError(entity)
        return cls(get_peer_id(entity), entity)

    @property
    def display_name(self, /) -> str | None:
        return self._display_name or None

    @property
    def id(self, /) -> int:
        return self._id

    @property
    def input_peer(self, /) -> TypeInputPeer:
        return self._input_peer

    _display_name: str | None
    _id: int
    _entity: PeerEntity
    _input_peer: TypeInputPeer

    __slots__ = '_display_name', '_entity', '_id', '_input_peer'

    def __new__(cls, id_: int, entity: PeerEntity, /) -> Self:
        self = super().__new__(cls)
        self._id, self._entity = id_, entity
        self._display_name = _entity_to_display_name(entity)
        input_peer = get_input_peer(entity, allow_self=False)
        assert input_peer is not None, entity
        self._input_peer = input_peer
        return self

    def __getnewargs__(self, /) -> tuple[int, PeerEntity]:
        return self._id, self._entity

    def __repr__(self, /) -> str:
        return f'{type(self).__qualname__}({self._id!r}, {self._entity!r})'

    def __str__(self, /) -> str:
        description = (
            f'with ID {self._id}'
            if self._display_name is None
            else f'{self._display_name!r} (ID {self._id})'
        )
        return f'peer {description}'


RawPeer: TypeAlias = str | int


def _entity_to_display_name(entity: PeerEntity, /) -> str | None:
    if isinstance(entity, User):
        first_name, last_name = entity.first_name, entity.last_name
        if last_name is not None and first_name is not None:
            return f'{first_name} {last_name}'
        if first_name is not None:
            assert isinstance(first_name, str), first_name
            return first_name
        if last_name is not None:
            assert isinstance(last_name, str), last_name
            return last_name
        return None
    assert isinstance(entity, Chat | Channel), entity
    title = entity.title
    assert isinstance(title, str), title
    return title
