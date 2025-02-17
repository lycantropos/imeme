from __future__ import annotations

import base64
import contextlib
import datetime as dt
import hashlib
import logging
from collections.abc import AsyncIterator, Callable, Generator, Mapping
from contextlib import ExitStack
from functools import partial
from pathlib import Path
from typing import Final, IO, TypeAlias

from telethon import TelegramClient  # type: ignore[import-untyped]
from telethon.tl.types import (  # type: ignore[import-untyped]
    Message,
    MessageService,
    TypeInputPeer,
    TypeMessageMedia,
)

from imeme._core.utils import DefaultMapping

from .constants import (
    MESSAGE_CACHE_DIRECTORY_NAME_FORMAT,
    MESSAGE_CACHE_FILE_NAME,
    MESSAGE_CACHE_FILE_SEPARATOR,
    MESSAGE_CACHE_HASH_FILE_NAME,
)
from .peer import Peer


async def fetch_message_media(
    message_media: TypeMessageMedia, /, *, client: TelegramClient
) -> bytes:
    result = await client.download_media(message_media, bytes)  # type: ignore[arg-type,unused-ignore]
    assert isinstance(result, bytes), message_media
    return result


async def fetch_newest_peer_message(
    peer: Peer, /, *, client: TelegramClient
) -> Message:
    async for candidate in client.iter_messages(peer.input_peer):
        if isinstance(candidate, Message):
            return candidate
    raise ValueError('No messages found.')


async def fetch_oldest_peer_message(
    peer: Peer, /, *, client: TelegramClient
) -> Message:
    async for candidate in client.iter_messages(peer.input_peer, reverse=True):
        if isinstance(candidate, Message):
            return candidate
    raise ValueError('No messages found.')


async def fetch_peer_messages_with_caching(
    peer: Peer,
    *,
    client: TelegramClient,
    logger: logging.Logger,
    start_message_date: dt.date,
    start_message_id: int,
    stop_message_date: dt.date,
    stop_message_id: int,
    message_batches_cache_directory_path: Path,
) -> AsyncIterator[Message]:
    with ExitStack() as stack:

        @contextlib.contextmanager
        def write_message_cache_file_hash_on_success(
            hasher: hashlib._Hash, hash_file_path: Path, /
        ) -> Generator[None, None, None]:
            yield
            hash_file_path.write_bytes(hasher.digest())

        def get_cache_writer(message_date: dt.date, /) -> _BytesWriter:
            message_cache_directory_path = (
                message_batches_cache_directory_path
                / message_date.strftime(MESSAGE_CACHE_DIRECTORY_NAME_FORMAT)
            )
            message_cache_directory_path.mkdir(exist_ok=True)
            message_cache_file_path = (
                message_cache_directory_path / MESSAGE_CACHE_FILE_NAME
            )
            stack.pop_all().close()
            hasher = hashlib.sha256()
            stack.enter_context(
                write_message_cache_file_hash_on_success(
                    hasher,
                    message_cache_directory_path
                    / MESSAGE_CACHE_HASH_FILE_NAME,
                )
            )
            message_cache_file = stack.enter_context(
                message_cache_file_path.open('wb')
            )
            return partial(
                write_bytes_updating_hash,
                file=message_cache_file,
                hasher=hasher,
            )

        def write_bytes_updating_hash(
            value: bytes, /, *, file: IO[bytes], hasher: hashlib._Hash
        ) -> None:
            file.write(value)
            hasher.update(value)

        messages_cache_writers: DefaultMapping[dt.date, _BytesWriter] = (
            DefaultMapping(get_cache_writer)
        )
        message_chunk_start_count = message_counter = 0
        message_chunk_start_date: dt.date | None = start_message_date
        message_chunk_stop_date: dt.date | None = stop_message_date
        async for message in _fetch_peer_messages(
            peer.input_peer,
            client=client,
            start_message_date=start_message_date,
            start_message_id=start_message_id,
            stop_message_date=stop_message_date,
            stop_message_id=stop_message_id,
        ):
            try:
                _sync_message(
                    message, messages_cache_writers=messages_cache_writers
                )
            except Exception:
                logger.exception(
                    'Failed message %s synchronization.', message.to_json()
                )
            else:
                yield message
            finally:
                message_counter += 1
                if message_counter % _MESSAGE_CHUNK_SIZE == 0:
                    message_datetime = message.date
                    message_chunk_start_date = (
                        None
                        if message_datetime is None
                        else message_datetime.date()
                    )
                    logger.debug(
                        (
                            'Finished loading of messages %s - %s '
                            'for %s from %s to %s.'
                        ),
                        message_chunk_start_count + 1,
                        message_counter,
                        peer,
                        message_chunk_start_date,
                        message_chunk_stop_date,
                    )
                    message_chunk_stop_date = message_chunk_start_date
                    message_chunk_start_count = message_counter
        if message_counter != message_chunk_start_count:
            logger.debug(
                'Finished loading of messages %s - %s for %s from %s to %s.',
                message_chunk_start_count + 1,
                message_counter,
                peer,
                message_chunk_start_date,
                message_chunk_stop_date,
            )


_MESSAGE_CHUNK_SIZE: Final[int] = 100

_BytesWriter: TypeAlias = Callable[[bytes], None]


async def _fetch_peer_messages(
    input_peer: TypeInputPeer,
    /,
    *,
    client: TelegramClient,
    start_message_date: dt.date,
    start_message_id: int,
    stop_message_date: dt.date,
    stop_message_id: int,
) -> AsyncIterator[Message]:
    assert stop_message_id == 0 or stop_message_id > start_message_id
    while True:
        last_message: Message | None = None
        async for message in client.iter_messages(
            input_peer, offset_id=stop_message_id, min_id=start_message_id - 1
        ):
            if isinstance(message, MessageService):
                continue
            assert start_message_id <= message.id, (
                start_message_id,
                message.id,
            )
            assert stop_message_id == 0 or message.id < stop_message_id, (
                message.id,
                stop_message_id,
            )
            assert start_message_date <= message.date.date(), (
                start_message_date,
                message.date,
            )
            assert message.date.date() < stop_message_date, (
                message.date,
                stop_message_date,
            )
            yield message
            last_message = message
        if last_message is None:
            return
        stop_message_id = last_message.id
        if stop_message_id == start_message_id + 1:
            break


def _serialize_peer_message(value: Message, /) -> bytes:
    return base64.b64encode(bytes(value))


def _sync_message(
    message: Message, *, messages_cache_writers: Mapping[dt.date, _BytesWriter]
) -> None:
    message_bytes = _serialize_peer_message(message)
    assert MESSAGE_CACHE_FILE_SEPARATOR not in message_bytes, message_bytes
    message_datetime = message.date
    assert message_datetime is not None, (
        f'Message {message.to_json()} has unset date.'
    )
    messages_cache_writers[message_datetime.date()](
        message_bytes + MESSAGE_CACHE_FILE_SEPARATOR
    )
