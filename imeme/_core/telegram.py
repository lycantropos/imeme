from __future__ import annotations

import base64
import datetime as dt
import enum
import logging
from collections.abc import AsyncIterator
from contextlib import ExitStack
from itertools import pairwise
from pathlib import Path
from typing import IO, Final

from telethon import TelegramClient  # type: ignore
from telethon.errors import TypeNotFoundError  # type: ignore
from telethon.extensions import BinaryReader  # type: ignore
from telethon.tl.types import (  # type: ignore
    InputPeerSelf,
    InputPeerUser,
    Message,
    TypeInputPeer,
)
from telethon.utils import get_peer_id  # type: ignore

from .utils import DefaultMapping, reverse_byte_stream

PEER_MESSAGE_CACHE_DIRECTORY_NAME_FORMAT: Final[str] = '%Y%m%d'
PEER_MESSAGE_CACHE_FILE_NAME: Final[str] = 'messages.raw'
PEER_MESSAGE_FILE_SEPARATOR: Final[bytes] = b'\n'


async def fetch_peer_messages(
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


async def sync_messages(
    peers: list[str | int],
    /,
    *,
    api_id: int,
    api_hash: str,
    logger: logging.Logger,
) -> None:
    cache_directory_path = Path.cwd() / '.cache'
    cache_directory_path.mkdir(exist_ok=True)
    client: TelegramClient
    async with TelegramClient(str(api_id), api_id, api_hash) as client:
        for peer in peers:
            input_peer = await client.get_input_entity(peer)
            if isinstance(input_peer, InputPeerSelf):
                input_peer = await client.get_me(input_peer=True)
                assert isinstance(input_peer, InputPeerUser), input_peer
            try:
                peer_id = get_peer_id(input_peer)
            except TypeError:
                logger.exception(
                    'Failed getting ID of peer %s, skipping.', input_peer
                )
                continue
            peer_cache_directory_path = cache_directory_path / str(peer_id)
            peer_cache_directory_path.mkdir(exist_ok=True)
            for (
                offset_peer_message_id,
                stop_peer_message_id,
                peer_message_cache_file_mode,
            ) in get_peer_message_id_limits_with_file_modes(
                peer_cache_directory_path
            ):
                with ExitStack() as stack:

                    def open_file(
                        path: Path,
                        *,
                        file_mode: PeerMessageCacheFileMode = (
                            peer_message_cache_file_mode
                        ),
                    ) -> IO[bytes]:
                        logger.info(f'processing {path.parent.name}')
                        return stack.enter_context(path.open(file_mode))

                    message_files: DefaultMapping[Path, IO[bytes]] = (
                        DefaultMapping(open_file)
                    )
                    async for message in fetch_peer_messages(
                        input_peer,
                        client=client,
                        offset_message_id=offset_peer_message_id,
                        stop_message_id=stop_peer_message_id,
                    ):
                        message_bytes = serialize_peer_message(message)
                        assert (
                            PEER_MESSAGE_FILE_SEPARATOR not in message_bytes
                        ), message_bytes
                        message_datetime = message.date
                        if message_datetime is None:
                            logger.warning(
                                'Message %s has unset date, skipping.', message
                            )
                            continue
                        message_cache_directory_path = (
                            peer_cache_directory_path
                            / (
                                message_datetime.date().strftime(
                                    PEER_MESSAGE_CACHE_DIRECTORY_NAME_FORMAT
                                )
                            )
                        )
                        message_cache_directory_path.mkdir(exist_ok=True)
                        message_files[
                            message_cache_directory_path
                            / PEER_MESSAGE_CACHE_FILE_NAME
                        ].write(message_bytes + PEER_MESSAGE_FILE_SEPARATOR)


@enum.unique
class PeerMessageCacheFileMode(str, enum.Enum):
    APPEND = 'ab'
    OVERWRITE = 'wb'


def get_peer_message_id_limits_with_file_modes(
    peer_cache_directory_path: Path, /
) -> list[tuple[int, int, PeerMessageCacheFileMode]]:
    if (
        len(
            peer_message_dates_with_cache_directory_paths := [
                (date, path)
                for path in peer_cache_directory_path.iterdir()
                if (
                    path.is_dir()
                    and (
                        (
                            date := safe_parse_date(
                                path.name,
                                PEER_MESSAGE_CACHE_DIRECTORY_NAME_FORMAT,
                            )
                        )
                        is not None
                    )
                    and is_valid_peer_message_cache_file_path(
                        path / PEER_MESSAGE_CACHE_FILE_NAME
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
                get_oldest_peer_message(
                    oldest_peer_message_cache_directory_path
                    / PEER_MESSAGE_CACHE_FILE_NAME
                ).id,
                0,
                PeerMessageCacheFileMode.APPEND,
            )
        ]
        for (older_date, older_date_directory_path), (
            newer_date,
            newer_date_directory_path,
        ) in pairwise(peer_message_dates_with_cache_directory_paths):
            assert newer_date > older_date, (newer_date, older_date)
            if (newer_date - older_date).days > 1:
                result.append(
                    (
                        get_oldest_peer_message(
                            newer_date_directory_path
                            / PEER_MESSAGE_CACHE_FILE_NAME
                        ).id,
                        get_newest_peer_message(
                            older_date_directory_path
                            / PEER_MESSAGE_CACHE_FILE_NAME
                        ).id,
                        PeerMessageCacheFileMode.OVERWRITE,
                    )
                )
        _, newest_peer_message_cache_directory_path = (
            peer_message_dates_with_cache_directory_paths[-1]
        )
        result.append(
            (
                0,
                get_oldest_peer_message(
                    newest_peer_message_cache_directory_path
                    / PEER_MESSAGE_CACHE_FILE_NAME
                ).id,
                PeerMessageCacheFileMode.OVERWRITE,
            )
        )
        return result
    else:
        return [(0, 0, PeerMessageCacheFileMode.OVERWRITE)]


def is_valid_peer_message_cache_file_path(path: Path, /) -> bool:
    try:
        file = path.open('rb')
    except OSError:
        return False
    with file:
        try:
            message_line = next(file)
        except StopIteration:
            return False
        message = checked_deserialize_peer_message_line(message_line)
        if message is None:
            return False
        for next_message_line in file:
            next_message = checked_deserialize_peer_message_line(
                next_message_line
            )
            if next_message is None or message.id <= next_message.id:
                return False
            message = next_message
        return True


def get_newest_peer_message(file_path: Path, /) -> Message:
    with file_path.open('rb') as file:
        result = deserialize_peer_message_line(next(file))
        try:
            next_message_line = next(file)
        except StopIteration:
            pass
        else:
            next_message = deserialize_peer_message_line(next_message_line)
            assert next_message.id < result.id, (
                file_path,
                next_message,
                result,
            )
        return result


def get_oldest_peer_message(file_path: Path, /) -> Message:
    with file_path.open('rb') as file:
        reversed_message_lines = reverse_byte_stream(
            file, lines_separator=PEER_MESSAGE_FILE_SEPARATOR
        )
        result = deserialize_peer_message_line(next(reversed_message_lines))
        try:
            previous_message_line = next(reversed_message_lines)
        except StopIteration:
            pass
        else:
            previous_message = deserialize_peer_message_line(
                previous_message_line
            )
            assert previous_message.id > result.id, (
                file_path,
                previous_message,
                result,
            )
        return result


def checked_deserialize_peer_message_line(value: bytes, /) -> Message | None:
    if not value.endswith(PEER_MESSAGE_FILE_SEPARATOR):
        return None
    try:
        return deserialize_peer_message_line(value)
    except (TypeError, TypeNotFoundError, ValueError):
        return None


def deserialize_peer_message_line(value: bytes, /) -> Message:
    result = BinaryReader(
        base64.b64decode(value[: -len(PEER_MESSAGE_FILE_SEPARATOR)])
    ).tgread_object()
    if not isinstance(result, Message):
        raise TypeError(result)
    return result


def serialize_peer_message(value: Message, /) -> bytes:
    return base64.b64encode(bytes(value))


def safe_parse_date(value: str, format_: str, /) -> dt.date | None:
    try:
        return dt.datetime.strptime(value, format_).date()
    except ValueError:
        return None
