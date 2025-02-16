from __future__ import annotations

from typing import Final

_MESSAGE_CACHE_DIRECTORY_NAME_FORMAT: Final[str] = '%Y%m%d'
_MESSAGE_CACHE_FILE_NAME: Final[str] = 'messages.raw'
_MESSAGE_CACHE_FILE_SEPARATOR: Final[bytes] = b'\n'
_MESSAGE_CACHE_HASH_FILE_NAME: Final[str] = 'messages.hash'
