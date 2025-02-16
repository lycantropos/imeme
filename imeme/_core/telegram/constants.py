from __future__ import annotations

from typing import Final

MESSAGE_CACHE_DIRECTORY_NAME_FORMAT: Final[str] = '%Y%m%d'
MESSAGE_CACHE_FILE_NAME: Final[str] = 'messages.raw'
MESSAGE_CACHE_FILE_SEPARATOR: Final[bytes] = b'\n'
MESSAGE_CACHE_HASH_FILE_NAME: Final[str] = 'messages.hash'
