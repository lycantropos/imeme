import asyncio
import contextlib
import datetime as dt
import logging
import shlex
from collections.abc import AsyncGenerator, Iterable
from pathlib import Path
from typing import Final, TypedDict

import httpx
from aiodocker import Docker
from telethon.utils import get_peer_id  # type: ignore[import-untyped]
from typesense import Client as TypesenseClient  # type: ignore[import-untyped]
from typesense.collection import Collection  # type: ignore[import-untyped]
from typesense.exceptions import ObjectNotFound  # type: ignore[import-untyped]

from imeme._core.json_wrapper import JsonWrapper
from imeme._core.telegram import Image, Peer, iter_images


@contextlib.asynccontextmanager
async def run_typesense_docker_container(
    *,
    cache_directory_path: Path,
    docker_client: Docker,
    logger: logging.Logger,
    typesense_image_version: str,
    typesense_api_key: str,
    typesense_port: int,
) -> AsyncGenerator[None, None]:
    typesense_image_name = f'typesense/typesense:{typesense_image_version}'
    async for docker_log_line in docker_client.images.pull(
        typesense_image_name, stream=True
    ):
        logger.debug(JsonWrapper(docker_log_line))
    typesense_data_host_directory_path = (
        cache_directory_path / 'typesense_data'
    )
    typesense_data_host_directory_path.mkdir(exist_ok=True)
    container = await docker_client.containers.create_or_replace(
        config={
            'Cmd': [
                '--data-dir',
                '/data',
                '--api-key',
                shlex.quote(typesense_api_key),
                '--enable-cors',
            ],
            'ExposedPorts': {'8108/tcp': {}},
            'Image': typesense_image_name,
            'HostConfig': {
                'Binds': [f'{typesense_data_host_directory_path}/:/data'],
                'PortBindings': {
                    '8108/tcp': [{'HostPort': str(typesense_port)}]
                },
            },
        },
        name='typesense',
    )
    await container.start()
    max_connection_attempts = 10
    last_error = None
    interval_between_connection_attempts_s = dt.timedelta(
        seconds=6
    ).total_seconds()
    async with httpx.AsyncClient(
        timeout=interval_between_connection_attempts_s
    ) as http_client:
        for _ in range(max_connection_attempts):
            await asyncio.sleep(interval_between_connection_attempts_s)
            response = await http_client.get(
                f'http://localhost:{typesense_port}/health'
            )
            try:
                response.raise_for_status()
            except httpx.HTTPStatusError as error:
                last_error = error
                continue
            else:
                await response.aread()
                assert response.json() == {'ok': True}, response.content
                break
        else:
            container_logs = await container.log(
                stdout=True, stderr=True, follow=False
            )
            container_log_string = '\n'.join(container_logs)
            raise RuntimeError(
                f'Failed to establish connection to typesense '
                f'after {max_connection_attempts} attempts '
                f'with {interval_between_connection_attempts_s}s interval, '
                f'container logs:\n{container_log_string}.'
            ) from last_error
    try:
        yield
    finally:
        await container.delete(force=True)


async def sync_typesense_telegram_image_documents(
    peers: list[Peer],
    /,
    *,
    cache_directory_path: Path,
    logger: logging.Logger,
    telegram_cache_directory_path: Path,
    typesense_api_key: str,
    typesense_image_version: str,
    typesense_port: int,
) -> None:
    async with (
        Docker() as docker_client,
        run_typesense_docker_container(
            cache_directory_path=cache_directory_path,
            docker_client=docker_client,
            logger=logger,
            typesense_image_version=typesense_image_version,
            typesense_api_key=typesense_api_key,
            typesense_port=typesense_port,
        ),
    ):
        typesense_client = TypesenseClient(
            {
                'api_key': typesense_api_key,
                'nodes': [
                    {
                        'host': 'localhost',
                        'port': str(typesense_port),
                        'protocol': 'http',
                    }
                ],
                'connection_timeout_seconds': 2,
            }
        )
        images_typesense_collection = typesense_client.collections[
            _IMAGES_TYPESENSE_COLLECTION_NAME
        ]
        assert isinstance(images_typesense_collection, Collection), (
            images_typesense_collection
        )
        try:
            retrieve_response = images_typesense_collection.retrieve()
        except ObjectNotFound:
            create_response = typesense_client.collections.create(
                {
                    'name': _IMAGES_TYPESENSE_COLLECTION_NAME,
                    'fields': [
                        {
                            'name': 'image_path',
                            'type': 'string',
                            'infix': True,
                        },
                        {'name': 'text', 'type': 'string[]', 'infix': True},
                        {'name': 'timestamp', 'type': 'float'},
                        {'name': 'peer_id', 'type': 'int64', 'facet': True},
                        {'name': 'day_timestamp', 'type': 'int64'},
                    ],
                    'default_sorting_field': 'timestamp',
                }
            )
            logger.debug(JsonWrapper(create_response))
        else:
            logger.debug(JsonWrapper(retrieve_response))
        upsert_responses = images_typesense_collection.documents.import_(
            _iter_telegram_image_documents(
                peers,
                cache_directory_path=telegram_cache_directory_path,
                logger=logger,
            ),
            {'action': 'upsert'},
            batch_size=1_000,
        )
        logger.debug(
            'Successfully upserted %s/%s typesense documents.',
            sum(
                response.get('success', False) for response in upsert_responses
            ),
            len(upsert_responses),
        )


_IMAGES_TYPESENSE_COLLECTION_NAME: Final[str] = 'images'


class _ImageDocument(TypedDict):
    day_timestamp: int
    image_path: str
    peer_id: int
    text: list[str]
    timestamp: float


def _datetime_to_epoch_timedelta(
    value: dt.datetime,
    /,
    _epoch_start: dt.datetime = dt.datetime(
        1970, 1, 1, tzinfo=dt.timezone.utc
    ),
) -> dt.timedelta:
    return value - _epoch_start


def _timedelta_to_seconds(
    value: dt.timedelta, /, _seconds_in_day: int = 86400
) -> int:
    assert value.microseconds == 0, value
    return value.days * _seconds_in_day + value.seconds


def _image_to_document(image: Image) -> _ImageDocument:
    message_datetime = image.message.date or dt.datetime.min
    return {
        'day_timestamp': _timedelta_to_seconds(
            _datetime_to_epoch_timedelta(
                dt.datetime.combine(
                    message_datetime.date(), dt.time.min, dt.timezone.utc
                )
            )
        ),
        'image_path': str(image.file_path),
        'peer_id': get_peer_id(image.message.peer_id),
        'text': [record['text'] for record in (image.ocr_result or [])],
        'timestamp': message_datetime.timestamp(),
    }


def _iter_telegram_image_documents(
    peers: list[Peer], /, *, cache_directory_path: Path, logger: logging.Logger
) -> Iterable[_ImageDocument]:
    for image in iter_images(peers, cache_directory_path=cache_directory_path):
        try:
            yield _image_to_document(image)
        except Exception:
            logger.debug(
                (
                    'Failed creation of typesense document from image %r, '
                    'skipping.'
                ),
                image,
                exc_info=True,
            )
            continue
