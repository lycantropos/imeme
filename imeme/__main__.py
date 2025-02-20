import asyncio
import enum
import logging
import logging.config
import os
import sys
from pathlib import Path
from typing import Any

import click
import tomli
from telethon import TelegramClient  # type: ignore[import-untyped]
from telethon.sessions import StringSession  # type: ignore[import-untyped]
from typing_extensions import Self

import imeme
from imeme._core.configuration import Configuration
from imeme._core.language import SupportedLanguage
from imeme._core.telegram import Peer, RawPeer, sync_images, sync_images_ocr

MIN_DEFAULT_LANGUAGES_COUNT = 1

MAX_DEFAULT_LANGUAGES_COUNT = 2


class Context:
    @property
    def cache_directory_path(self, /) -> Path:
        return self._cache_directory_path

    @property
    def logger(self, /) -> logging.Logger:
        return self._logger

    _cache_directory_path: Path
    _logger: logging.Logger

    __slots__ = '_cache_directory_path', '_logger'

    def __new__(
        cls, *, cache_directory_path: Path, logger: logging.Logger
    ) -> Self:
        self = super().__new__(cls)
        (self._cache_directory_path, self._logger) = (
            cache_directory_path,
            logger,
        )
        return self


@click.option(
    '--cache-directory-path',
    default=Path.cwd() / '.cache',
    help='Path to a directory to cache data to.',
    type=click.Path(
        dir_okay=True, writable=True, file_okay=False, path_type=Path
    ),
)
@click.option(
    '--logging-configuration-file-path',
    default=Path.cwd() / 'logging.toml',
    help='Path to a file (in TOML format) with logging configurations.',
    type=click.Path(
        dir_okay=False,
        exists=True,
        file_okay=True,
        path_type=Path,
        readable=True,
    ),
)
@click.option(
    '-v',
    '--verbose',
    count=True,
    help='Controls logs verbosity level.',
    show_default=False,
)
@click.version_option(imeme.__version__, message='%(version)s')
@click.group(context_settings={'show_default': True})
@click.pass_context
def main(
    context: click.Context,
    /,
    *,
    cache_directory_path: Path,
    logging_configuration_file_path: Path,
    verbose: int,
) -> None:
    cache_directory_path.mkdir(exist_ok=True)
    logging_configuration = tomli.loads(
        logging_configuration_file_path.read_text('utf-8')
    )
    logging.config.dictConfig(logging_configuration)
    logger = logging.getLogger(__name__)
    new_level = max(1, logger.level - 10 * verbose)
    logger.setLevel(new_level)
    context.obj = Context(
        cache_directory_path=cache_directory_path, logger=logger
    )


class SyncTarget(str, enum.Enum):
    IMAGES = 'images'
    IMAGE_OCR = 'image_ocr'


def _get_max_available_cpus() -> int:
    result = None
    if sys.version_info >= (3, 13):
        result = os.process_cpu_count()
    elif sys.platform != 'darwin' and sys.platform != 'win32':
        result = len(os.sched_getaffinity(0))
    return result or max(
        (os.cpu_count() or 1)
        # 1 for main process + 2 just in case
        - 3,
        1,
    )


@click.option(
    '--configuration-file-path',
    default=Path.cwd() / 'sync.toml',
    help='Path to a file (in TOML format) with configurations.',
    type=click.Path(
        dir_okay=False, exists=True, file_okay=True, path_type=Path
    ),
)
@click.option(
    '--max-subprocesses-count',
    default=_get_max_available_cpus(),
    help='Maximum number of subprocesses to spawn.',
    type=click.IntRange(1),
)
@click.argument('targets', nargs=-1, type=click.Choice(tuple(SyncTarget)))
@main.command
@click.pass_obj
def sync(
    context: Context,
    /,
    *,
    configuration_file_path: Path,
    max_subprocesses_count: int,
    targets: list[SyncTarget],
) -> None:
    configuration: Configuration[Any] = Configuration.from_toml_file_path(
        configuration_file_path
    )
    telegram_configuration_section = configuration.get_section('telegram')
    peers_list = telegram_configuration_section.get_list('peers')
    if len(peers_list) == 0:
        raise ValueError(f'Invalid {peers_list}: should not be empty.')
    raw_peers: list[RawPeer] = []
    logger = context.logger
    for raw_peer_field in peers_list:
        try:
            raw_peer = raw_peer_field.extract_exact(RawPeer)
        except TypeError:
            logger.exception('Failed raw peer deserialization, skipping.')
            continue
        else:
            raw_peers.append(raw_peer)
    default_languages_list = telegram_configuration_section.get_list(
        'default_languages'
    )
    default_languages = [
        SupportedLanguage(language_field.extract_exact(str))
        for language_field in default_languages_list
    ]
    if not (
        MIN_DEFAULT_LANGUAGES_COUNT
        <= len(default_languages)
        <= MAX_DEFAULT_LANGUAGES_COUNT
    ):
        raise ValueError(
            f'Invalid {default_languages_list} size: '
            f'expected to be from {MIN_DEFAULT_LANGUAGES_COUNT} '
            f'to {MAX_DEFAULT_LANGUAGES_COUNT}, '
            f'but got {len(default_languages)}.'
        )
    if len(default_languages) != len(set(default_languages)):
        raise ValueError(
            f'Invalid {default_languages_list} elements: '
            'expected to be distinct.'
        )
    asyncio.run(
        _sync(
            raw_peers,
            api_id=telegram_configuration_section['api_id'].extract_exact(int),
            api_hash=telegram_configuration_section['api_hash'].extract_exact(
                str
            ),
            cache_directory_path=context.cache_directory_path,
            default_languages=default_languages,
            logger=logger,
            max_subprocesses_count=max_subprocesses_count,
            targets=targets,
        )
    )


async def _sync(
    raw_peers: list[RawPeer],
    /,
    *,
    api_id: int,
    api_hash: str,
    cache_directory_path: Path,
    default_languages: list[SupportedLanguage],
    logger: logging.Logger,
    max_subprocesses_count: int,
    targets: list[SyncTarget],
) -> None:
    sync_all = len(targets) == 0
    telegram_cache_directory_path = cache_directory_path / 'telegram'
    client: TelegramClient
    session_string_file_path = Path(f'{api_id}.session.string')
    try:
        session_string = session_string_file_path.read_text()
    except OSError:
        session_string = None
    async with TelegramClient(
        (
            StringSession()
            if session_string is None
            else StringSession(session_string)
        ),
        api_id,
        api_hash,
    ) as client:
        if session_string is None:
            session = client.session
            assert isinstance(session, StringSession), session
            session_string = session.save()
            assert len(session_string) > 0, session_string
            session_string_file_path.write_text(session_string)
        peers: list[Peer] = []
        for raw_peer in raw_peers:
            try:
                peer = await Peer.from_raw(raw_peer, client=client)
            except Exception:
                logger.exception(
                    'Failed resolution of peer %r, skipping.', raw_peer
                )
                continue
            else:
                peers.append(peer)
    if sync_all or SyncTarget.IMAGES in targets:
        telegram_cache_directory_path.mkdir(exist_ok=True)
        async with TelegramClient(
            StringSession(session_string), api_id, api_hash
        ) as client:
            await sync_images(
                peers,
                client=client,
                cache_directory_path=telegram_cache_directory_path,
                logger=logger,
            )
    if sync_all or SyncTarget.IMAGE_OCR in targets:
        sync_images_ocr(
            peers,
            cache_directory_path=telegram_cache_directory_path,
            default_languages=default_languages,
            logger=logger,
            max_subprocesses_count=max_subprocesses_count,
        )


main()
