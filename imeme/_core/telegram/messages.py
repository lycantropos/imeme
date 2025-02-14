from __future__ import annotations

import base64
import contextlib
import datetime as dt
import hashlib
import logging
import os
from collections.abc import AsyncIterator, Generator, Mapping
from contextlib import ExitStack
from itertools import pairwise
from pathlib import Path
from typing import IO, Final, TypeAlias

from telethon import TelegramClient  # type: ignore[import-untyped]
from telethon.extensions import BinaryReader  # type: ignore[import-untyped]
from telethon.tl.types import (  # type: ignore[import-untyped]
    InputPeerSelf,
    InputPeerUser,
    Message,
    MessageService,
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
    peers_messages_cache_directory_path = (
        cache_directory_path / _MESSAGES_CACHE_DIRECTORY_NAME
    )
    peers_messages_cache_directory_path.mkdir(exist_ok=True)
    client: TelegramClient
    async with TelegramClient(str(api_id), api_id, api_hash) as client:
        for raw_peer in raw_peers:
            logger.info('Starting messages synchronization for %r.', raw_peer)
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


_MAX_TELEGRAM_MESSAGE_DATE: Final[dt.date] = dt.date.max
_MESSAGE_CACHE_DIRECTORY_NAME_FORMAT: Final[str] = '%Y%m%d'
_MESSAGE_CACHE_FILE_NAME: Final[str] = 'messages.raw'
_MESSAGE_CACHE_FILE_SEPARATOR: Final[bytes] = b'\n'
_MESSAGE_CACHE_HASH_FILE_NAME: Final[str] = 'messages.hash'
_MESSAGE_CHUNK_SIZE: Final[int] = 1_000
_MESSAGES_CACHE_DIRECTORY_NAME = 'messages'
_MIN_TELEGRAM_MESSAGE_DATE: Final[dt.date] = dt.date(2013, 1, 1)


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


class _PeerMessageBatchLimits:
    @property
    def start_date(self, /) -> dt.date:
        return self._start_date

    @property
    def start_id(self, /) -> int:
        return self._start_id

    @property
    def stop_date(self, /) -> dt.date:
        return self._stop_date

    @property
    def stop_id(self, /) -> int:
        return self._stop_id

    _start_date: dt.date
    _start_id: int
    _stop_date: dt.date
    _stop_id: int

    __slots__ = '_start_date', '_start_id', '_stop_date', '_stop_id'

    def __new__(
        cls,
        /,
        *,
        start_date: dt.date,
        start_id: int,
        stop_date: dt.date,
        stop_id: int,
    ) -> Self:
        error_messages = []
        if start_id < 0:
            error_messages.append(f'{start_id} should be non-negative.')
        if stop_id < 0:
            error_messages.append(f'{stop_id} should be non-negative.')
        if stop_id != 0 and start_id >= stop_id:
            error_messages.append(f'{start_id} should be less than {stop_id}.')
        if start_date >= stop_date:
            error_messages.append(
                f'{start_date} should be less than {stop_date}.'
            )
        if len(error_messages) > 0:
            raise ValueError('\n'.join(error_messages))
        self = super().__new__(cls)
        self._start_date, self._start_id, self._stop_date, self._stop_id = (
            start_date,
            start_id,
            stop_date,
            stop_id,
        )
        return self


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
            input_peer, offset_id=stop_message_id, min_id=start_message_id
        ):
            if isinstance(message, MessageService):
                continue
            assert start_message_id < message.id, (
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
            assert message.date.date() <= stop_message_date, (
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


def _calculate_file_hash(
    file_path: Path, /, chunk_bytes_count: int = 4096
) -> bytes:
    sha256_hash = hashlib.sha256()
    with file_path.open('rb') as file:
        for byte_block in iter(lambda: file.read(chunk_bytes_count), b''):
            sha256_hash.update(byte_block)
    return sha256_hash.digest()


def _checked_deserialize_peer_message_line(value: bytes, /) -> Message | None:
    if not value.endswith(_MESSAGE_CACHE_FILE_SEPARATOR):
        return None
    try:
        return _deserialize_peer_message_line(value)
    except Exception as _e:
        return None


def _deserialize_peer_message_line(value: bytes, /) -> Message:
    result = BinaryReader(
        base64.b64decode(value[: -len(_MESSAGE_CACHE_FILE_SEPARATOR)])
    ).tgread_object()
    if not isinstance(result, Message):
        raise TypeError(result)
    return result


def _is_valid_message_cache_file_contents(
    file_path: Path, /, *, expected_peer_id: int, expected_date: dt.date
) -> bool:
    try:
        file = file_path.open('rb')
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
        if not _is_valid_message(
            message,
            expected_date=expected_date,
            expected_peer_id=expected_peer_id,
        ):
            return False
        for next_message_line in file:
            next_message = _checked_deserialize_peer_message_line(
                next_message_line
            )
            if next_message is None:
                return False
            if not _is_valid_message(
                message,
                expected_date=expected_date,
                expected_peer_id=expected_peer_id,
            ):
                return False
            if next_message is None or message.id <= next_message.id:
                return False
            message = next_message
        return True


def _is_valid_message(
    value: Message, /, *, expected_date: dt.date, expected_peer_id: int
) -> bool:
    try:
        message_peer_id = get_peer_id(value.peer_id)
    except Exception:
        return False
    if message_peer_id != expected_peer_id:
        return False
    message_datetime = value.date
    return not (
        message_datetime is None or message_datetime.date() != expected_date
    )


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
            file, lines_separator=_MESSAGE_CACHE_FILE_SEPARATOR
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


def _safe_read_bytes_from_file(file_path: Path, /) -> bytes | None:
    try:
        return file_path.read_bytes()
    except OSError:
        return None


def _safe_get_path_stat(path: Path, /) -> os.stat_result | None:
    try:
        return path.stat()
    except OSError:
        return None


def _serialize_peer_message(value: Message, /) -> bytes:
    return base64.b64encode(bytes(value))


async def _sync_message(
    message: Message, *, messages_cache_files: Mapping[dt.date, IO[bytes]]
) -> None:
    message_bytes = _serialize_peer_message(message)
    assert _MESSAGE_CACHE_FILE_SEPARATOR not in message_bytes, message_bytes
    message_datetime = message.date
    assert message_datetime is not None, (
        f'Message {message.stringify()} has unset date.'
    )
    messages_cache_files[message_datetime.date()].write(
        message_bytes + _MESSAGE_CACHE_FILE_SEPARATOR
    )


async def _sync_peer_message_batch(
    peer: _Peer,
    *,
    client: TelegramClient,
    input_peer: TypeInputPeer,
    logger: logging.Logger,
    message_batch_limits: _PeerMessageBatchLimits,
    message_batches_cache_directory_path: Path,
) -> None:
    with ExitStack() as stack:

        @contextlib.contextmanager
        def write_message_cache_file_hash_on_success(
            cache_file_path: Path, hash_file_path: Path, /
        ) -> Generator[None, None, None]:
            yield
            hash_file_path.write_bytes(_calculate_file_hash(cache_file_path))

        def open_cache_file(message_date: dt.date, /) -> IO[bytes]:
            message_cache_directory_path = (
                message_batches_cache_directory_path
                / message_date.strftime(_MESSAGE_CACHE_DIRECTORY_NAME_FORMAT)
            )
            message_cache_directory_path.mkdir(exist_ok=True)
            message_cache_file_path = (
                message_cache_directory_path / _MESSAGE_CACHE_FILE_NAME
            )
            stack.pop_all().close()
            stack.enter_context(
                write_message_cache_file_hash_on_success(
                    message_cache_file_path,
                    message_cache_directory_path
                    / _MESSAGE_CACHE_HASH_FILE_NAME,
                )
            )
            return stack.enter_context(message_cache_file_path.open('wb'))

        messages_cache_files: DefaultMapping[dt.date, IO[bytes]] = (
            DefaultMapping(open_cache_file)
        )
        message_counter = 0
        message_chunk_stop_date: dt.date | None = (
            message_batch_limits.stop_date
        )
        async for message in _fetch_peer_messages(
            input_peer,
            client=client,
            start_message_date=message_batch_limits.start_date,
            start_message_id=message_batch_limits.start_id,
            stop_message_date=message_batch_limits.stop_date,
            stop_message_id=message_batch_limits.stop_id,
        ):
            try:
                await _sync_message(
                    message, messages_cache_files=messages_cache_files
                )
            except Exception:
                logger.exception(
                    'Failed message %s synchronization, skipping.',
                    message.stringify(),
                )
                continue
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
                            'Finished synchronization of (%s, %s] '
                            'messages for %s from %s to %s.'
                        ),
                        message_counter - _MESSAGE_CHUNK_SIZE,
                        message_counter,
                        peer,
                        message_chunk_start_date,
                        message_chunk_stop_date,
                    )
                    message_chunk_stop_date = message_chunk_start_date


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
    message_batches_cache_directory_path = cache_directory_path / str(peer.id)
    message_batches_cache_directory_path.mkdir(exist_ok=True)
    for message_batch_limits in _to_peer_message_batch_limits(
        peer,
        message_batches_cache_directory_path=(
            message_batches_cache_directory_path
        ),
    ):
        logger.debug(
            'Starting messages synchronization for %s of batch from %s to %s.',
            peer,
            message_batch_limits.start_date,
            message_batch_limits.stop_date,
        )
        try:
            await _sync_peer_message_batch(
                peer,
                client=client,
                input_peer=input_peer,
                logger=logger,
                message_batch_limits=message_batch_limits,
                message_batches_cache_directory_path=(
                    message_batches_cache_directory_path
                ),
            )
        except Exception:
            logger.exception(
                (
                    'Failed messages synchronization for %s '
                    'of batch from %s to %s.'
                ),
                peer,
                message_batch_limits.start_date,
                message_batch_limits.stop_date,
            )
            continue
        else:
            logger.debug(
                (
                    'Successfully finished messages synchronization for %s '
                    'of batch from %s to %s.'
                ),
                peer,
                message_batch_limits.start_date,
                message_batch_limits.stop_date,
            )


def _to_peer_message_batch_limits(
    peer: _Peer, *, message_batches_cache_directory_path: Path
) -> list[_PeerMessageBatchLimits]:
    if (
        len(
            message_dates_with_cache_directory_paths := [
                (date, path)
                for path in message_batches_cache_directory_path.iterdir()
                if (
                    path.is_dir()
                    and (
                        (
                            date := _safe_parse_date(
                                path.name, _MESSAGE_CACHE_DIRECTORY_NAME_FORMAT
                            )
                        )
                        is not None
                    )
                    and _has_message_cache_file_valid_hash(
                        message_cache_file_path := (
                            path / _MESSAGE_CACHE_FILE_NAME
                        ),
                        path / _MESSAGE_CACHE_HASH_FILE_NAME,
                    )
                    and _is_valid_message_cache_file_contents(
                        message_cache_file_path,
                        expected_peer_id=peer.id,
                        expected_date=date,
                    )
                )
            ]
        )
        > 1
    ):
        message_dates_with_cache_directory_paths.sort()
        oldest_message_date, oldest_message_cache_directory_path = (
            message_dates_with_cache_directory_paths[0]
        )
        result = [
            _PeerMessageBatchLimits(
                start_id=0,
                start_date=_MIN_TELEGRAM_MESSAGE_DATE,
                stop_id=_load_newest_peer_message(
                    oldest_message_cache_directory_path
                    / _MESSAGE_CACHE_FILE_NAME
                ).id,
                stop_date=oldest_message_date,
            )
        ]
        for (older_date, older_date_directory_path), (
            newer_date,
            newer_date_directory_path,
        ) in pairwise(message_dates_with_cache_directory_paths):
            assert newer_date > older_date, (newer_date, older_date)
            if (newer_date - older_date).days > 1:
                newer_date_lowest_message_id = _load_oldest_peer_message(
                    newer_date_directory_path / _MESSAGE_CACHE_FILE_NAME
                ).id
                older_date_highest_message_id = _load_newest_peer_message(
                    older_date_directory_path / _MESSAGE_CACHE_FILE_NAME
                ).id
                if _no_messages_missing := (
                    newer_date_lowest_message_id
                    == older_date_highest_message_id + 1
                ):
                    continue
                result.append(
                    _PeerMessageBatchLimits(
                        start_id=older_date_highest_message_id,
                        start_date=older_date,
                        stop_id=newer_date_lowest_message_id,
                        stop_date=newer_date,
                    )
                )
        newest_message_date, newest_message_cache_directory_path = (
            message_dates_with_cache_directory_paths[-1]
        )
        result.append(
            _PeerMessageBatchLimits(
                start_date=newest_message_date,
                start_id=_load_oldest_peer_message(
                    newest_message_cache_directory_path
                    / _MESSAGE_CACHE_FILE_NAME
                ).id,
                stop_id=0,
                stop_date=_MAX_TELEGRAM_MESSAGE_DATE,
            )
        )
        return result
    else:
        return [
            _PeerMessageBatchLimits(
                start_date=_MIN_TELEGRAM_MESSAGE_DATE,
                start_id=0,
                stop_date=_MAX_TELEGRAM_MESSAGE_DATE,
                stop_id=0,
            )
        ]


def _has_message_cache_file_valid_hash(
    message_cache_file_path: Path, message_cache_hash_file_path: Path, /
) -> bool:
    return (
        (
            _expected_message_cache_file_hash := _safe_read_bytes_from_file(
                message_cache_hash_file_path
            )
        )
        is not None
    ) and (
        _calculate_file_hash(message_cache_file_path)
        == _expected_message_cache_file_hash
    )
