import json
from collections.abc import Callable
from functools import cache
from pathlib import Path
from typing import IO, Literal, TypeAlias, TypedDict, cast

import easyocr  # type: ignore[import-untyped]

from .caching import calculate_file_hash, to_hasher
from .language import SupportedLanguage

_BoundingBox: TypeAlias = list[list[int]]


class ImageOcrRecord(TypedDict):
    bounding_box: _BoundingBox
    confidence: float
    text: str


@cache
def languages_to_image_text_recognizer(
    languages: tuple[SupportedLanguage], /
) -> Callable[[bytes], list[ImageOcrRecord]]:
    languages_set = frozenset(languages)
    reader_languages: list[Literal['en', 'ru']]
    if languages_set == {SupportedLanguage.ENGLISH}:
        reader_languages = ['en']
    elif languages_set == {SupportedLanguage.RUSSIAN} or languages_set == {
        SupportedLanguage.ENGLISH,
        SupportedLanguage.RUSSIAN,
    }:
        reader_languages = ['en', 'ru']
    else:
        raise ValueError(f'Unsupported languages: {languages}.')
    implementation = cast(
        Callable[[bytes], list[tuple[_BoundingBox, str, float]]],
        easyocr.Reader(reader_languages).readtext,
    )

    def recognize_text_in_image(
        image_content: bytes, /
    ) -> list[ImageOcrRecord]:
        return [
            {
                'bounding_box': [
                    [int(coordinate) for coordinate in coordinates]
                    for coordinates in bounding_box
                ],
                'text': text,
                'confidence': confidence,
            }
            for bounding_box, text, confidence in implementation(image_content)
        ]

    return recognize_text_in_image


def sync_image_ocr(
    image_file_path: Path,
    /,
    *,
    encoding: str = 'utf-8',
    image_text_recognizer: Callable[[bytes], list[ImageOcrRecord]],
) -> None:
    with image_file_path.open('rb') as image_file:
        image_ocr_file_path, image_ocr_hash_file_path = (
            _to_image_ocr_file_path(image_file_path),
            _to_image_ocr_hash_file_path(image_file_path),
        )
        if _is_image_ocr_file_in_cache(
            image_ocr_file_path, image_ocr_hash_file_path
        ):
            return
        image_ocr_result = json.dumps(image_text_recognizer(image_file.read()))
        image_ocr_file_path.write_text(image_ocr_result, encoding=encoding)
        image_ocr_hash_file_path.write_bytes(
            to_hasher(image_ocr_result.encode(encoding)).digest()
        )


def load_image_ocr_result_from_cache(
    image_file_path: Path, /
) -> list[ImageOcrRecord] | None:
    try:
        image_ocr_file = _to_image_ocr_file_path(image_file_path).open('rb')
    except OSError:
        return None
    else:
        with image_ocr_file:
            if not _does_image_ocr_file_has_valid_hash(
                image_ocr_file, _to_image_ocr_hash_file_path(image_file_path)
            ):
                return None
            image_ocr_file.seek(0)
            result = json.load(image_ocr_file)
            assert isinstance(result, list), result
            return result


def _does_image_ocr_file_has_valid_hash(
    image_ocr_file: IO[bytes], image_ocr_hash_file_path: Path
) -> bool:
    try:
        expected_image_ocr_hash = image_ocr_hash_file_path.read_bytes()
    except OSError:
        return False
    else:
        return calculate_file_hash(image_ocr_file) == expected_image_ocr_hash


def _is_image_ocr_file_in_cache(
    image_ocr_file_path: Path, image_ocr_hash_file_path: Path
) -> bool:
    try:
        image_ocr_file = image_ocr_file_path.open('rb')
    except OSError:
        return False
    else:
        with image_ocr_file:
            return _does_image_ocr_file_has_valid_hash(
                image_ocr_file, image_ocr_hash_file_path
            )


def _to_image_ocr_hash_file_path(image_file_path: Path, /) -> Path:
    return image_file_path.with_suffix('.ocr.hash')


def _to_image_ocr_file_path(image_file_path: Path, /) -> Path:
    return image_file_path.with_suffix('.ocr.json')
