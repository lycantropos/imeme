from __future__ import annotations

import hashlib
from collections.abc import Callable
from typing import IO


def calculate_file_hash(
    file: IO[bytes], /, chunk_bytes_count: int = 4096
) -> bytes:
    hasher = to_hasher()
    for byte_block in iter(lambda: file.read(chunk_bytes_count), b''):
        hasher.update(byte_block)
    return hasher.digest()


to_hasher: Callable[..., hashlib._Hash] = hashlib.sha256
