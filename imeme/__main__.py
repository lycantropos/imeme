import asyncio
import logging.config
from pathlib import Path
from typing import Any

import click
import tomli
from typing_extensions import Self

from imeme._core.configuration import Configuration
from imeme._core.telegram import sync_messages


class Context:
    @property
    def logger(self, /) -> logging.Logger:
        return self._logger

    _logger: logging.Logger

    __slots__ = ('_logger',)

    def __new__(cls, *, logger: logging.Logger) -> Self:
        self = super().__new__(cls)
        self._logger = logger
        return self


@click.option(
    '--logging-configuration-file-path',
    help='Path to a file (in TOML format) with logging configurations.',
    default=Path('logging.toml'),
    type=click.Path(
        dir_okay=False, exists=True, file_okay=True, path_type=Path
    ),
)
@click.option('-v', '--verbose', help='Logs verbosity level.', count=True)
@click.group
@click.pass_context
def main(
    context: click.Context,
    /,
    *,
    logging_configuration_file_path: Path,
    verbose: int,
) -> None:
    configuration = tomli.loads(
        logging_configuration_file_path.read_text('utf-8')
    )
    logging.config.dictConfig(configuration)
    logger = logging.getLogger(__name__)
    new_level = max(1, logger.level - 10 * verbose)
    logger.setLevel(new_level)
    context.obj = Context(logger=logger)


@click.option(
    '--configuration-file-path',
    help='Path to a file (in TOML format) with configurations.',
    default=Path('sync.toml'),
    type=click.Path(
        dir_okay=False, exists=True, file_okay=True, path_type=Path
    ),
)
@main.command
@click.pass_obj
def sync(context: Context, /, *, configuration_file_path: Path) -> None:
    configuration: Configuration[Any] = Configuration.from_toml_file_path(
        configuration_file_path
    )
    telegram_configuration_section = configuration.get_section('telegram')
    peers = [
        peer.extract_exact(str | int)
        for peer in telegram_configuration_section.get_list('peers')
    ]
    asyncio.run(
        sync_messages(
            peers,
            api_id=telegram_configuration_section['api_id'].extract_exact(int),
            api_hash=telegram_configuration_section['api_hash'].extract_exact(
                str
            ),
            logger=context.logger,
        )
    )


main()
