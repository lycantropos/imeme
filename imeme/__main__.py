import asyncio
import enum
import logging.config
from pathlib import Path
from typing import Any

import click
import tomli
from typing_extensions import Self

import imeme
from imeme._core.configuration import Configuration
from imeme._core.telegram import RawPeer, sync_images


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
        self._cache_directory_path, self._logger = cache_directory_path, logger
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


@click.option(
    '--configuration-file-path',
    default=Path.cwd() / 'sync.toml',
    help='Path to a file (in TOML format) with configurations.',
    type=click.Path(
        dir_okay=False, exists=True, file_okay=True, path_type=Path
    ),
)
@click.argument('targets', nargs=-1, type=click.Choice(tuple(SyncTarget)))
@main.command
@click.pass_obj
def sync(
    context: Context,
    /,
    *,
    configuration_file_path: Path,
    targets: list[SyncTarget],
) -> None:
    configuration: Configuration[Any] = Configuration.from_toml_file_path(
        configuration_file_path
    )
    telegram_configuration_section = configuration.get_section('telegram')
    peers_list = telegram_configuration_section.get_list('peers')
    if len(peers_list) == 0:
        raise ValueError(f'Invalid {peers_list}: should not be empty.')
    peers = [peer.extract_exact(RawPeer) for peer in peers_list]
    sync_all = len(targets) == 0
    if sync_all or SyncTarget.IMAGES in targets:
        telegram_cache_directory_path = (
            context.cache_directory_path / 'telegram'
        )
        telegram_cache_directory_path.mkdir(exist_ok=True)
        asyncio.run(
            sync_images(
                peers,
                api_id=telegram_configuration_section['api_id'].extract_exact(
                    int
                ),
                api_hash=telegram_configuration_section[
                    'api_hash'
                ].extract_exact(str),
                cache_directory_path=telegram_cache_directory_path,
                logger=context.logger,
            )
        )


main()
