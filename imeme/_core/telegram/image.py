from pathlib import Path

from telethon.tl.types import Message  # type: ignore[import-untyped]
from typing_extensions import Self

from imeme._core.text_recognition import ImageOcrRecord


class Image:
    @property
    def file_path(self, /) -> Path:
        return self._file_path

    @property
    def message(self, /) -> Message:
        return self._message

    @property
    def ocr_result(self, /) -> list[ImageOcrRecord] | None:
        return self._ocr_result

    _file_path: Path
    _message: Message
    _ocr_result: list[ImageOcrRecord] | None

    __slots__ = '_file_path', '_message', '_ocr_result'

    def __new__(
        cls,
        /,
        *,
        file_path: Path,
        message: Message,
        ocr_result: list[ImageOcrRecord] | None,
    ) -> Self:
        self = super().__new__(cls)
        self._file_path, self._message, self._ocr_result = (
            file_path,
            message,
            ocr_result,
        )
        return self

    def __repr__(self, /) -> str:
        return (
            f'{type(self).__qualname__}('
            f'file_path={self._file_path}, '
            f'message={self._message}, '
            f'ocr_result={self._ocr_result}'
            f')'
        )
