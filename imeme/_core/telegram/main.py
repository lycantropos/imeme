from __future__ import annotations

import base64
import datetime as dt
import hashlib
import logging
import os
from collections.abc import Callable
from pathlib import Path
from typing import Final, IO

from telethon import TelegramClient  # type: ignore[import-untyped]
from telethon.errors import BadRequestError  # type: ignore[import-untyped]
from telethon.extensions import BinaryReader  # type: ignore[import-untyped]
from telethon.tl.types import (  # type: ignore[import-untyped]
    Document,
    Message,
    MessageMediaDocument,
    MessageMediaPhoto,
    Photo,
    PhotoSize,
    PhotoSizeProgressive,
)

from imeme._core.utils import reverse_byte_stream

from .constants import (
    MESSAGE_CACHE_DIRECTORY_NAME_FORMAT,
    MESSAGE_CACHE_FILE_NAME,
    MESSAGE_CACHE_FILE_SEPARATOR,
    MESSAGE_CACHE_HASH_FILE_NAME,
)
from .fetching import (
    fetch_message_media,
    fetch_newest_peer_message,
    fetch_oldest_peer_message,
    fetch_peer_messages_with_caching,
)
from .peer import Peer, RawPeer


async def sync_images(
    raw_peers: list[RawPeer],
    /,
    *,
    api_id: int,
    api_hash: str,
    cache_directory_path: Path,
    logger: logging.Logger,
) -> None:
    client: TelegramClient
    async with TelegramClient(str(api_id), api_id, api_hash) as client:
        for raw_peer in raw_peers:
            logger.info('Starting images synchronization for %r.', raw_peer)
            try:
                await _sync_peer_images(
                    raw_peer,
                    client=client,
                    logger=logger,
                    cache_directory_path=cache_directory_path,
                )
            except Exception:
                logger.exception(
                    'Failed images synchronization for %r.', raw_peer
                )
                continue
            else:
                logger.info(
                    'Successfully finished images synchronization for %r.',
                    raw_peer,
                )


_IMAGE_DOCUMENT_MIME_TYPE_PREFIX: Final[str] = 'image/'
_MAX_TELEGRAM_MESSAGE_DATE: Final[dt.date] = dt.date.max
_MIN_TELEGRAM_MESSAGE_DATE: Final[dt.date] = dt.date(2013, 1, 1)


def _calculate_file_hash(
    file: IO[bytes], /, chunk_bytes_count: int = 4096
) -> bytes:
    hasher = _to_hasher()
    for byte_block in iter(lambda: file.read(chunk_bytes_count), b''):
        hasher.update(byte_block)
    return hasher.digest()


def _deserialize_peer_message_line(value: bytes, /) -> Message:
    result = BinaryReader(
        base64.b64decode(value[: -len(MESSAGE_CACHE_FILE_SEPARATOR)])
    ).tgread_object()
    if not isinstance(result, Message):
        raise TypeError(result)
    return result


def _is_file_in_cache(
    *, cache_file_path: Path, cache_hash_file_path: Path, file_size: int
) -> bool:
    try:
        cache_file = cache_file_path.open('rb')
    except OSError:
        pass
    else:
        with cache_file:
            try:
                expected_cache_file_hash = cache_hash_file_path.read_bytes()
            except OSError:
                pass
            else:
                if (
                    _calculate_file_hash(cache_file)
                    == expected_cache_file_hash
                ) and (cache_file.seek(0, os.SEEK_END) == file_size):
                    return True
    return False


def _load_newest_cached_peer_message(file: IO[bytes], /) -> Message:
    if file.tell() != 0:
        file.seek(0)
    result = _deserialize_peer_message_line(next(file))
    try:
        next_message_line = next(file)
    except StopIteration:
        pass
    else:
        next_message = _deserialize_peer_message_line(next_message_line)
        assert next_message.id < result.id, (file.name, next_message, result)
    return result


def _load_oldest_cached_peer_message(file: IO[bytes], /) -> Message:
    reversed_message_lines = reverse_byte_stream(
        file, lines_separator=MESSAGE_CACHE_FILE_SEPARATOR
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
            file.name,
            previous_message,
            result,
        )
    return result


def _photo_to_image_file_size(value: Photo, /) -> int:
    candidates: list[int] = []
    for size_data in value.sizes:
        if isinstance(size_data, PhotoSize):
            candidates.append(size_data.size)
        elif isinstance(size_data, PhotoSizeProgressive):
            candidates.append(max(size_data.sizes))
    return max(candidates)


def _safe_parse_date(value: str, format_: str, /) -> dt.date | None:
    try:
        return dt.datetime.strptime(value, format_).date()
    except ValueError:
        return None


async def _safe_sync_message_image(
    message: Message,
    /,
    *,
    client: TelegramClient,
    logger: logging.Logger,
    message_batches_cache_directory_path: Path,
) -> None:
    try:
        await _sync_message_image(
            message,
            client=client,
            message_batches_cache_directory_path=(
                message_batches_cache_directory_path
            ),
        )
    except Exception:
        logger.debug(
            'Failed message %s image synchronization, skipping.',
            message.to_json(),
        )


async def _sync_message_image(
    message: Message,
    /,
    *,
    client: TelegramClient,
    message_batches_cache_directory_path: Path,
) -> None:
    message_media = message.media
    message_datetime = message.date
    assert message_datetime is not None, (
        f'Message {message.to_json()} has unset date.'
    )
    image_cache_directory_path = (
        message_batches_cache_directory_path
        / message_datetime.date().strftime(MESSAGE_CACHE_DIRECTORY_NAME_FORMAT)
    )
    if isinstance(message_media, MessageMediaPhoto):
        message_photo = message_media.photo
        assert isinstance(message_photo, Photo), message
        image_cache_file_name_without_extension = (
            f'{message.id}_{message_photo.id}'
        )
        image_cache_file_path = (
            image_cache_directory_path
            / f'{image_cache_file_name_without_extension}.jpg'
        )
        image_cache_hash_file_path = (
            image_cache_directory_path
            / f'{image_cache_file_name_without_extension}.hash'
        )
        if _is_file_in_cache(
            cache_file_path=image_cache_file_path,
            cache_hash_file_path=image_cache_hash_file_path,
            file_size=_photo_to_image_file_size(message_photo),
        ):
            return
        image_bytes = await fetch_message_media(message_media, client=client)
        image_cache_directory_path.mkdir(exist_ok=True)
        image_cache_file_path.write_bytes(image_bytes)
        image_cache_hash_file_path.write_bytes(
            _to_hasher(image_bytes).digest()
        )
    elif isinstance(message_media, MessageMediaDocument):
        message_document = message_media.document
        assert isinstance(message_document, Document), message
        message_document_mime_type = message_document.mime_type
        if message_document_mime_type.startswith(
            _IMAGE_DOCUMENT_MIME_TYPE_PREFIX
        ):
            image_cache_file_name_without_extension = (
                f'{message.id}_{message_document.id}'
            )
            image_cache_file_extension = message_document_mime_type[
                len(_IMAGE_DOCUMENT_MIME_TYPE_PREFIX) :
            ]
            image_cache_file_path = image_cache_directory_path / (
                f'{image_cache_file_name_without_extension}'
                f'.{image_cache_file_extension}'
            )
            image_cache_hash_file_path = (
                image_cache_directory_path
                / f'{image_cache_file_name_without_extension}.hash'
            )
            if _is_file_in_cache(
                cache_file_path=image_cache_file_path,
                cache_hash_file_path=image_cache_hash_file_path,
                file_size=message_document.size,
            ):
                return
            image_bytes = await fetch_message_media(
                message_media, client=client
            )
            image_cache_directory_path.mkdir(exist_ok=True)
            image_cache_file_path.write_bytes(image_bytes)
            image_cache_hash_file_path.write_bytes(
                _to_hasher(image_bytes).digest()
            )


async def _sync_peer_images(
    raw_peer: RawPeer,
    /,
    *,
    client: TelegramClient,
    logger: logging.Logger,
    cache_directory_path: Path,
) -> None:
    peer = await Peer.from_raw(raw_peer, client=client)
    oldest_message = await fetch_oldest_peer_message(peer, client=client)
    older_date = (
        oldest_message_datetime.date() - dt.timedelta(days=1)
        if (oldest_message_datetime := oldest_message.date) is not None
        else _MIN_TELEGRAM_MESSAGE_DATE
    )
    older_highest_message_id = oldest_message.id - 1
    older_lowest_message_id = 0
    message_batches_cache_directory_path = cache_directory_path / str(peer.id)
    message_batches_cache_directory_path.mkdir(exist_ok=True)
    for candidate_date, candidate_message_cache_directory_path in sorted(
        [
            (date, path)
            for path in message_batches_cache_directory_path.iterdir()
            if (
                path.is_dir()
                and (
                    (
                        date := _safe_parse_date(
                            path.name, MESSAGE_CACHE_DIRECTORY_NAME_FORMAT
                        )
                    )
                    is not None
                )
            )
        ]
    ):
        candidate_message_cache_file_path = (
            candidate_message_cache_directory_path / MESSAGE_CACHE_FILE_NAME
        )
        try:
            candidate_message_cache_file = (
                candidate_message_cache_file_path.open('rb')
            )
        except OSError:
            continue
        with candidate_message_cache_file:
            candidate_message_cache_hash_file_path = (
                candidate_message_cache_directory_path
                / MESSAGE_CACHE_HASH_FILE_NAME
            )
            try:
                expected_candidate_message_cache_file_hash = (
                    candidate_message_cache_hash_file_path
                ).read_bytes()
            except OSError:
                continue
            if _candidate_message_cache_file_has_invalid_hash := (
                _calculate_file_hash(candidate_message_cache_file)
                != expected_candidate_message_cache_file_hash
            ):
                continue
            candidate_highest_message_id = _load_newest_cached_peer_message(
                candidate_message_cache_file
            ).id
            candidate_lowest_message_id = _load_oldest_cached_peer_message(
                candidate_message_cache_file
            ).id
            if (candidate_date - older_date).days > 1 and (
                candidate_lowest_message_id != older_highest_message_id + 1
            ):
                async for message in fetch_peer_messages_with_caching(
                    peer,
                    client=client,
                    logger=logger,
                    message_batches_cache_directory_path=(
                        message_batches_cache_directory_path
                    ),
                    start_message_id=older_highest_message_id + 1,
                    start_message_date=older_date + dt.timedelta(days=1),
                    stop_message_id=candidate_lowest_message_id,
                    stop_message_date=candidate_date,
                ):
                    await _safe_sync_message_image(
                        message,
                        client=client,
                        logger=logger,
                        message_batches_cache_directory_path=(
                            message_batches_cache_directory_path
                        ),
                    )
            candidate_message_cache_file.seek(0)
            try:
                for message_line_number, message_line in enumerate(
                    candidate_message_cache_file, start=1
                ):
                    try:
                        message = _deserialize_peer_message_line(message_line)
                    except Exception:
                        logger.debug(
                            (
                                'Failed to deserialize message from file %r '
                                'line %r with number %s, skipping.'
                            ),
                            str(candidate_message_cache_file_path),
                            message_line,
                            message_line_number,
                            exc_info=True,
                        )
                        continue
                    await _sync_message_image(
                        message,
                        client=client,
                        message_batches_cache_directory_path=(
                            message_batches_cache_directory_path
                        ),
                    )
            except BadRequestError:
                # access expired, cache is invalid
                async for message in fetch_peer_messages_with_caching(
                    peer,
                    client=client,
                    logger=logger,
                    message_batches_cache_directory_path=(
                        message_batches_cache_directory_path
                    ),
                    start_message_id=candidate_lowest_message_id,
                    start_message_date=candidate_date,
                    stop_message_id=candidate_highest_message_id + 1,
                    stop_message_date=candidate_date + dt.timedelta(days=1),
                ):
                    await _safe_sync_message_image(
                        message,
                        client=client,
                        logger=logger,
                        message_batches_cache_directory_path=(
                            message_batches_cache_directory_path
                        ),
                    )
            older_date = candidate_date
            older_highest_message_id = candidate_highest_message_id
            older_lowest_message_id = candidate_lowest_message_id
    newest_message = await fetch_newest_peer_message(peer, client=client)
    if newest_message.id != older_highest_message_id:
        async for message in fetch_peer_messages_with_caching(
            peer,
            client=client,
            logger=logger,
            message_batches_cache_directory_path=(
                message_batches_cache_directory_path
            ),
            start_message_id=older_lowest_message_id,
            start_message_date=older_date,
            stop_message_id=0,
            stop_message_date=_MAX_TELEGRAM_MESSAGE_DATE,
        ):
            await _safe_sync_message_image(
                message,
                client=client,
                logger=logger,
                message_batches_cache_directory_path=(
                    message_batches_cache_directory_path
                ),
            )


_to_hasher: Callable[..., hashlib._Hash] = hashlib.sha256
