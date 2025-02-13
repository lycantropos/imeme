from __future__ import annotations

import base64
import datetime as dt
import enum
import logging
from collections.abc import AsyncIterator, Mapping
from contextlib import ExitStack
from itertools import pairwise
from pathlib import Path
from typing import IO, Final, TypeAlias

from telethon import TelegramClient  # type: ignore[import-untyped]
from telethon.errors import TypeNotFoundError  # type: ignore[import-untyped]
from telethon.extensions import BinaryReader  # type: ignore[import-untyped]
from telethon.tl.types import (  # type: ignore[import-untyped]
    InputPeerSelf,
    InputPeerUser,
    Message,
    TypeInputPeer,
)
from telethon.utils import get_peer_id  # type: ignore[import-untyped]
from typing_extensions import Self

from imeme._core.utils import DefaultMapping, reverse_byte_stream

RawPeer: TypeAlias = str | int


async def sync_messages(
    raw_peers: list[RawPeer],
    /,
    *,
    api_id: int,
    api_hash: str,
    cache_directory_path: Path,
    logger: logging.Logger,
) -> None:
    peers_messages_cache_directory_path = cache_directory_path / 'messages'
    peers_messages_cache_directory_path.mkdir(exist_ok=True)
    client: TelegramClient
    async with TelegramClient(str(api_id), api_id, api_hash) as client:
        for raw_peer in raw_peers:
            logger.info('Started messages synchronization for %r.', raw_peer)
            try:
                await _sync_peer_messages(
                    raw_peer,
                    client=client,
                    logger=logger,
                    cache_directory_path=peers_messages_cache_directory_path,
                )
            except Exception:
                logger.exception(
                    'Failed messages synchronization for %r.', raw_peer
                )
                continue
            else:
                logger.info(
                    'Successfully finished messages synchronization for %r.',
                    raw_peer,
                )


_PEER_MESSAGE_CACHE_DIRECTORY_NAME_FORMAT: Final[str] = '%Y%m%d'
_PEER_MESSAGE_CACHE_FILE_NAME: Final[str] = 'messages.raw'
_PEER_MESSAGE_FILE_SEPARATOR: Final[bytes] = b'\n'


class _Peer:
    @property
    def id(self, /) -> int:
        return self._id

    __slots__ = '_id'

    @classmethod
    def from_input_peer(cls, input_peer: TypeInputPeer, /) -> Self:
        return cls(get_peer_id(input_peer))

    def __init__(self, id_: int, /) -> None:
        self._id = id_

    def __repr__(self, /) -> str:
        return f'{type(self).__qualname__}({self._id})'

    def __str__(self, /) -> str:
        return f'peer with ID {self._id}'


@enum.unique
class _PeerMessageCacheFileMode(str, enum.Enum):
    APPEND = 'ab'
    OVERWRITE = 'wb'


async def _fetch_peer_messages(
    input_peer: TypeInputPeer,
    /,
    *,
    client: TelegramClient,
    offset_message_id: int,
    stop_message_id: int,
) -> AsyncIterator[Message]:
    assert offset_message_id == 0 or offset_message_id > stop_message_id
    while True:
        last_message: Message | None = None
        async for message in client.iter_messages(
            input_peer, offset_id=offset_message_id, min_id=stop_message_id
        ):
            assert offset_message_id == 0 or message.id < offset_message_id
            assert stop_message_id < message.id
            yield message
            last_message = message
        if last_message is None:
            return
        offset_message_id = last_message.id
        if offset_message_id == stop_message_id + 1:
            break


def _get_peer_message_id_limits_with_file_modes(
    peer_messages_cache_directory_path: Path, /
) -> list[tuple[int, int, _PeerMessageCacheFileMode]]:
    if (
        len(
            peer_message_dates_with_cache_directory_paths := [
                (date, path)
                for path in peer_messages_cache_directory_path.iterdir()
                if (
                    path.is_dir()
                    and (
                        (
                            date := _safe_parse_date(
                                path.name,
                                _PEER_MESSAGE_CACHE_DIRECTORY_NAME_FORMAT,
                            )
                        )
                        is not None
                    )
                    and _is_valid_peer_message_cache_file_path(
                        path / _PEER_MESSAGE_CACHE_FILE_NAME
                    )
                )
            ]
        )
        > 1
    ):
        peer_message_dates_with_cache_directory_paths.sort()
        _, oldest_peer_message_cache_directory_path = (
            peer_message_dates_with_cache_directory_paths[0]
        )
        result = [
            (
                _load_oldest_peer_message(
                    oldest_peer_message_cache_directory_path
                    / _PEER_MESSAGE_CACHE_FILE_NAME
                ).id,
                0,
                _PeerMessageCacheFileMode.APPEND,
            )
        ]
        for (older_date, older_date_directory_path), (
            newer_date,
            newer_date_directory_path,
        ) in pairwise(peer_message_dates_with_cache_directory_paths):
            assert newer_date > older_date, (newer_date, older_date)
            if (newer_date - older_date).days > 1:
                newer_date_oldest_message_id = _load_oldest_peer_message(
                    newer_date_directory_path / _PEER_MESSAGE_CACHE_FILE_NAME
                ).id
                older_date_newest_message_id = _load_newest_peer_message(
                    older_date_directory_path / _PEER_MESSAGE_CACHE_FILE_NAME
                ).id
                if _multiple_days_passed_without_posting := (
                    newer_date_oldest_message_id
                    == older_date_newest_message_id + 1
                ):
                    continue
                result.append(
                    (
                        newer_date_oldest_message_id,
                        older_date_newest_message_id,
                        _PeerMessageCacheFileMode.OVERWRITE,
                    )
                )
        _, newest_peer_message_cache_directory_path = (
            peer_message_dates_with_cache_directory_paths[-1]
        )
        result.append(
            (
                0,
                _load_oldest_peer_message(
                    newest_peer_message_cache_directory_path
                    / _PEER_MESSAGE_CACHE_FILE_NAME
                ).id,
                _PeerMessageCacheFileMode.OVERWRITE,
            )
        )
        return result
    else:
        return [(0, 0, _PeerMessageCacheFileMode.OVERWRITE)]


def _checked_deserialize_peer_message_line(value: bytes, /) -> Message | None:
    if not value.endswith(_PEER_MESSAGE_FILE_SEPARATOR):
        return None
    try:
        return _deserialize_peer_message_line(value)
    except (TypeError, TypeNotFoundError, ValueError):
        return None


def _deserialize_peer_message_line(value: bytes, /) -> Message:
    result = BinaryReader(
        base64.b64decode(value[: -len(_PEER_MESSAGE_FILE_SEPARATOR)])
    ).tgread_object()
    if not isinstance(result, Message):
        raise TypeError(result)
    return result


def _is_valid_peer_message_cache_file_path(path: Path, /) -> bool:
    try:
        file = path.open('rb')
    except OSError:
        return False
    with file:
        try:
            message_line = next(file)
        except StopIteration:
            return False
        message = _checked_deserialize_peer_message_line(message_line)
        if message is None:
            return False
        for next_message_line in file:
            next_message = _checked_deserialize_peer_message_line(
                next_message_line
            )
            if next_message is None or message.id <= next_message.id:
                return False
            message = next_message
        return True


def _load_newest_peer_message(file_path: Path, /) -> Message:
    with file_path.open('rb') as file:
        result = _deserialize_peer_message_line(next(file))
        try:
            next_message_line = next(file)
        except StopIteration:
            pass
        else:
            next_message = _deserialize_peer_message_line(next_message_line)
            assert next_message.id < result.id, (
                file_path,
                next_message,
                result,
            )
        return result


def _load_oldest_peer_message(file_path: Path, /) -> Message:
    with file_path.open('rb') as file:
        reversed_message_lines = reverse_byte_stream(
            file, lines_separator=_PEER_MESSAGE_FILE_SEPARATOR
        )
        result = _deserialize_peer_message_line(next(reversed_message_lines))
        try:
            previous_message_line = next(reversed_message_lines)
        except StopIteration:
            pass
        else:
            previous_message = _deserialize_peer_message_line(
                previous_message_line
            )
            assert previous_message.id > result.id, (
                file_path,
                previous_message,
                result,
            )
        return result


def _safe_parse_date(value: str, format_: str, /) -> dt.date | None:
    try:
        return dt.datetime.strptime(value, format_).date()
    except ValueError:
        return None


def _serialize_peer_message(value: Message, /) -> bytes:
    return base64.b64encode(bytes(value))


async def _sync_peer_messages(
    raw_peer: RawPeer,
    /,
    *,
    client: TelegramClient,
    logger: logging.Logger,
    cache_directory_path: Path,
) -> None:
    input_peer = await client.get_input_entity(raw_peer)
    if isinstance(input_peer, InputPeerSelf):
        input_peer = await client.get_me(input_peer=True)
        assert isinstance(input_peer, InputPeerUser), input_peer.stringify()
    peer = _Peer.from_input_peer(input_peer)
    peer_messages_cache_directory_path = cache_directory_path / str(peer.id)
    peer_messages_cache_directory_path.mkdir(exist_ok=True)
    for (
        offset_peer_message_id,
        stop_peer_message_id,
        peer_message_cache_file_mode,
    ) in _get_peer_message_id_limits_with_file_modes(
        peer_messages_cache_directory_path
    ):
        with ExitStack() as stack:

            def open_file(
                path: Path,
                *,
                file_mode: _PeerMessageCacheFileMode = (
                    peer_message_cache_file_mode
                ),
            ) -> IO[bytes]:
                logger.debug('%s: processing %r', peer, path.parent.name)
                return stack.enter_context(path.open(file_mode))

            peer_message_files: DefaultMapping[Path, IO[bytes]] = (
                DefaultMapping(open_file)
            )
            async for peer_message in _fetch_peer_messages(
                input_peer,
                client=client,
                offset_message_id=offset_peer_message_id,
                stop_message_id=stop_peer_message_id,
            ):
                try:
                    await _synchronize_peer_message(
                        peer_message,
                        message_files=peer_message_files,
                        messages_cache_directory_path=(
                            peer_messages_cache_directory_path
                        ),
                    )
                except Exception:
                    logger.exception(
                        'Failed message %s synchronization, skipping.',
                        peer_message.stringify(),
                    )
                    return


async def _synchronize_peer_message(
    message: Message,
    *,
    message_files: Mapping[Path, IO[bytes]],
    messages_cache_directory_path: Path,
) -> None:
    peer_message_bytes = _serialize_peer_message(message)
    assert _PEER_MESSAGE_FILE_SEPARATOR not in peer_message_bytes, (
        peer_message_bytes
    )
    peer_message_datetime = message.date
    assert peer_message_datetime is not None, (
        f'Peer message {message.stringify()} has unset date.'
    )
    peer_message_cache_directory_path = messages_cache_directory_path / (
        peer_message_datetime.date().strftime(
            _PEER_MESSAGE_CACHE_DIRECTORY_NAME_FORMAT
        )
    )
    peer_message_cache_directory_path.mkdir(exist_ok=True)
    message_files[
        peer_message_cache_directory_path / _PEER_MESSAGE_CACHE_FILE_NAME
    ].write(peer_message_bytes + _PEER_MESSAGE_FILE_SEPARATOR)
