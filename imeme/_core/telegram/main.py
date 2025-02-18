from __future__ import annotations

import base64
import datetime as dt
import logging
import multiprocessing
import os
import re
import unicodedata
from collections import Counter
from collections.abc import Iterable
from concurrent.futures import as_completed
from concurrent.futures.process import ProcessPoolExecutor
from logging.handlers import QueueHandler, QueueListener
from pathlib import Path
from queue import Queue
from typing import Any, Final, IO

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

from imeme._core.caching import calculate_file_hash, to_hasher
from imeme._core.language import SupportedLanguage, SupportedLanguageCategory
from imeme._core.text_recognition import sync_image_ocr
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
from .peer import Peer


def classify_peer_languages(
    peer: Peer,
    /,
    *,
    cache_directory_path: Path,
    default: list[SupportedLanguage],
) -> list[SupportedLanguage]:
    character_categories_counter = (
        Counter()
        if peer.display_name is None
        else _to_string_raw_categories_counter(peer.display_name)
    )
    for message in _iter_cached_peer_messages(
        peer, cache_directory_path=cache_directory_path
    ):
        character_categories_counter += _to_string_raw_categories_counter(
            message.message
        )
    total_characters_count = character_categories_counter.total()
    return (
        sorted(
            SupportedLanguage.ENGLISH
            if (
                SupportedLanguageCategory(key)
                is SupportedLanguageCategory.LATIN
            )
            else SupportedLanguage.RUSSIAN
            for key, value in character_categories_counter.items()
            if (
                SupportedLanguageCategory.is_supported(key)
                and value / total_characters_count > 0.05
            )
        )
        or default
    )


async def sync_images(
    peers: list[Peer],
    /,
    *,
    client: TelegramClient,
    cache_directory_path: Path,
    logger: logging.Logger,
) -> None:
    for peer in peers:
        logger.info('Starting images synchronization for %s.', peer)
        try:
            await _sync_peer_images(
                peer,
                client=client,
                logger=logger,
                cache_directory_path=cache_directory_path,
            )
        except Exception:
            logger.exception('Failed images synchronization for %s.', peer)
            continue
        else:
            logger.info(
                'Successfully finished images synchronization for %s.', peer
            )


def sync_images_ocr(
    peers: list[Peer],
    /,
    *,
    cache_directory_path: Path,
    default_languages: list[SupportedLanguage],
    logger: logging.Logger,
    max_subprocesses_count: int,
) -> None:
    logger.debug(
        'Starting %s-process images OCR synchronization',
        max_subprocesses_count,
    )
    if max_subprocesses_count == 1:
        for peer in peers:
            _sync_peer_images_ocr(
                peer,
                cache_directory_path=cache_directory_path,
                default_languages=default_languages,
                log_queue=None,
                logger_level=logger.level,
                logger_name=logger.name,
            )
    else:
        with (
            ProcessPoolExecutor(max_subprocesses_count) as pool,
            multiprocessing.Manager() as manager,
        ):
            log_queue = manager.Queue()
            listener = QueueListener(log_queue, *logger.handlers)
            listener.start()
            peers_by_futures = {
                pool.submit(
                    _sync_peer_images_ocr,
                    peer,
                    cache_directory_path=cache_directory_path,
                    default_languages=default_languages,
                    log_queue=log_queue,
                    logger_level=logger.level,
                    logger_name=str(peer),
                ): peer
                for peer in peers
            }
            for future in as_completed(peers_by_futures.keys()):
                try:
                    result = future.result()
                except Exception:
                    logger.exception(
                        'Failed images OCR synchronization for %s, skipping.',
                        peers_by_futures[future],
                    )
                    continue
                else:
                    assert result is None, result
            logger.debug('Waiting for listener thread to finish...')
            listener.stop()


def _iter_cached_peer_image_file_paths(
    peer: Peer, /, *, cache_directory_path: Path
) -> Iterable[Path]:
    message_batches_cache_directory_path = cache_directory_path / str(peer.id)
    for message in _iter_cached_peer_messages(
        peer, cache_directory_path=cache_directory_path
    ):
        message_media = message.media
        message_datetime = message.date
        if message_datetime is None:
            continue
        image_cache_directory_path = (
            message_batches_cache_directory_path
            / message_datetime.date().strftime(
                MESSAGE_CACHE_DIRECTORY_NAME_FORMAT
            )
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
                yield image_cache_file_path
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
                    yield image_cache_file_path


def _iter_cached_peer_messages(
    peer: Peer, /, *, cache_directory_path: Path
) -> Iterable[Message]:
    message_batches_cache_directory_path = cache_directory_path / str(peer.id)
    message_batches_cache_directory_path.mkdir(exist_ok=True)
    for _, candidate_message_cache_directory_path in sorted(
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
                calculate_file_hash(candidate_message_cache_file)
                != expected_candidate_message_cache_file_hash
            ):
                continue
            candidate_message_cache_file.seek(0)
            for message_line in candidate_message_cache_file:
                try:
                    yield _deserialize_peer_message_line(message_line)
                except Exception:
                    continue


_IMAGE_DOCUMENT_MIME_TYPE_PREFIX: Final[str] = 'image/'
_IMAGE_OCR_CHUNK_SIZE: Final[int] = 4
_MAX_TELEGRAM_MESSAGE_DATE: Final[dt.date] = dt.date.max
_MIN_TELEGRAM_MESSAGE_DATE: Final[dt.date] = dt.date(2013, 1, 1)


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
                    calculate_file_hash(cache_file) == expected_cache_file_hash
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


def _to_string_raw_categories_counter(
    value: str,
    /,
    *,
    url_pattern: re.Pattern[str] = re.compile(r'https?://\S+'),
) -> Counter[str]:
    return Counter(
        unicodedata.name(character).split(maxsplit=1)[0].lower()
        for character in url_pattern.sub('', value)
        if character.isalpha()
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
        image_cache_hash_file_path.write_bytes(to_hasher(image_bytes).digest())
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
                to_hasher(image_bytes).digest()
            )


async def _sync_peer_images(
    peer: Peer,
    /,
    *,
    client: TelegramClient,
    logger: logging.Logger,
    cache_directory_path: Path,
) -> None:
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
                calculate_file_hash(candidate_message_cache_file)
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


def _sync_peer_images_ocr(
    peer: Peer,
    /,
    *,
    cache_directory_path: Path,
    default_languages: list[SupportedLanguage],
    log_queue: Queue[Any] | None,
    logger_level: int,
    logger_name: str,
) -> None:
    logger = logging.getLogger(logger_name)
    if log_queue is not None:
        queue_handler = QueueHandler(log_queue)
        logger.addHandler(queue_handler)
        logger.setLevel(logger_level)
    logger.info('Starting images OCR for %s.', peer)
    languages = classify_peer_languages(
        peer,
        cache_directory_path=cache_directory_path,
        default=default_languages,
    )
    logger.debug('Detected languages for %s: %s', peer, ', '.join(languages))
    image_chunk_start_count = image_counter = 0
    for image_counter, image_file_path in enumerate(
        _iter_cached_peer_image_file_paths(
            peer, cache_directory_path=cache_directory_path
        ),
        start=1,
    ):
        try:
            sync_image_ocr(image_file_path, languages=languages)
        except Exception:
            logger.debug(
                'Failed OCR of image with path %s for %s, skipping.',
                image_file_path,
                peer,
            )
            continue
        finally:
            if image_counter % _IMAGE_OCR_CHUNK_SIZE == 0:
                logger.debug(
                    'Finished OCR of images %s - %s for %s.',
                    image_chunk_start_count + 1,
                    image_counter,
                    peer,
                )
                image_chunk_start_count = image_counter
    if image_counter != image_chunk_start_count:
        logger.debug(
            'Finished OCR of images %s - %s for %s.',
            image_chunk_start_count + 1,
            image_counter,
            peer,
        )
    logger.info('Successfully finished images OCR for %s.', peer)
