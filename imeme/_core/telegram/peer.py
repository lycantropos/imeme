from __future__ import annotations

from typing import TypeAlias

from telethon import TelegramClient  # type: ignore[import-untyped]
from telethon.tl.types import (  # type: ignore[import-untyped]
    InputPeerSelf,
    InputPeerUser,
    TypeInputPeer,
)
from telethon.utils import get_peer_id  # type: ignore[import-untyped]
from typing_extensions import Self

RawPeer: TypeAlias = str | int


class Peer:
    @classmethod
    async def from_raw(
        cls, raw: RawPeer, /, *, client: TelegramClient
    ) -> Self:
        input_peer = await client.get_input_entity(raw)
        if isinstance(input_peer, InputPeerSelf):
            input_peer = await client.get_me(input_peer=True)
            assert isinstance(input_peer, InputPeerUser), input_peer
        return cls(get_peer_id(input_peer), input_peer)

    @property
    def id(self, /) -> int:
        return self._id

    @property
    def input_peer(self, /) -> TypeInputPeer:
        return self._input_peer

    _id: int
    _input_peer: TypeInputPeer
    __slots__ = '_id', '_input_peer'

    def __new__(cls, id_: int, input_peer: TypeInputPeer, /) -> Self:
        self = super().__new__(cls)
        self._id, self._input_peer = id_, input_peer
        return self

    def __repr__(self, /) -> str:
        return f'{type(self).__qualname__}({self._id!r}, {self._input_peer!r})'

    def __str__(self, /) -> str:
        return f'peer with ID {self._id}'
