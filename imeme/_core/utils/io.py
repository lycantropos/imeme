import io
import itertools
import operator
from collections.abc import Iterator
from typing import BinaryIO


def reverse_byte_stream(
    value: BinaryIO,
    /,
    *,
    batch_size: int = io.DEFAULT_BUFFER_SIZE,
    code_unit_size: int = 1,
    keep_lines_separator: bool = True,
    lines_separator: bytes,
) -> Iterator[bytes]:
    def split_lines(
        byte_sequence: bytes, _separator: bytes = lines_separator, /
    ) -> list[bytes]:
        result: list[bytes] = []
        part = bytearray()
        offset = 0
        add_part = result.append
        while offset < len(byte_sequence):
            if byte_sequence[offset : offset + len(_separator)] != _separator:
                part += byte_sequence[offset : offset + code_unit_size]
                offset += code_unit_size
            else:
                add_part(bytes(part + keep_lines_separator * _separator))
                part.clear()
                offset += len(_separator)
        if len(part) > 0:
            add_part(bytes(part))
        return result

    stream_size = value.seek(0, io.SEEK_END)
    if stream_size == 0:
        return
    batches_count = -((-stream_size) // batch_size)
    remaining_bytes_indicator = itertools.islice(
        itertools.accumulate(
            itertools.chain([stream_size], itertools.repeat(batch_size)),
            operator.sub,
        ),
        batches_count,
    )
    remaining_bytes_count = next(remaining_bytes_indicator)

    def read_batch(position: int) -> bytes:
        result = read_batch_from_end(
            value, size=batch_size, end_position=position
        )
        while result.startswith(lines_separator):
            try:
                position = next(remaining_bytes_indicator)
            except StopIteration:
                break
            result = (
                read_batch_from_end(
                    value, size=batch_size, end_position=position
                )
                + result
            )
        return result

    batch = read_batch(remaining_bytes_count)
    segment, *lines = split_lines(batch)
    yield from reversed(lines)
    for remaining_bytes_count in remaining_bytes_indicator:
        batch = read_batch(remaining_bytes_count)
        lines = split_lines(batch)
        if batch.endswith(lines_separator):
            yield segment
        else:
            lines[-1] += segment
        segment, *lines = lines
        yield from reversed(lines)
    yield segment


def read_batch_from_end(
    byte_stream: BinaryIO, *, size: int, end_position: int
) -> bytes:
    if end_position > size:
        offset = end_position - size
    else:
        offset = 0
        size = end_position
    byte_stream.seek(offset)
    return byte_stream.read(size)
