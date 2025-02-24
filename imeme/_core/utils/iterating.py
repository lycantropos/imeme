import itertools
from collections.abc import Iterable, Sequence
from typing import TypeVar

_T = TypeVar('_T')


def to_chunks(
    iterable: Iterable[_T], /, *, size: int
) -> Iterable[Sequence[_T]]:
    iterator = iter(iterable)
    yield from iter(lambda: tuple(itertools.islice(iterator, size)), ())
