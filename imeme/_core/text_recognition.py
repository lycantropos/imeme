import json
from collections.abc import Callable
from functools import cache
from pathlib import Path
from typing import Literal, TypeAlias, TypedDict, cast

import easyocr  # type: ignore[import-untyped]

from .caching import calculate_file_hash, to_hasher
from .language import SupportedLanguage

_BoundingBox: TypeAlias = list[list[int]]


class OcrRecord(TypedDict):
    bounding_box: _BoundingBox
    confidence: float
    text: str


@cache
def languages_to_recognizer(
    languages: tuple[SupportedLanguage], /
) -> Callable[[bytes], list[OcrRecord]]:
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

    def recognize_text(image_content: bytes, /) -> list[OcrRecord]:
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

    return recognize_text


def sync_image_ocr(
    image_file_path: Path,
    /,
    *,
    encoding: str = 'utf-8',
    recognizer: Callable[[bytes], list[OcrRecord]],
) -> None:
    image_ocr_hash_file_path = image_file_path.with_suffix('.ocr.hash')
    with image_file_path.open('rb') as image_file:
        image_ocr_file_path = image_file_path.with_suffix('.ocr.json')
        try:
            image_ocr_file = image_ocr_file_path.open('rb')
        except OSError:
            pass
        else:
            with image_ocr_file:
                try:
                    expected_image_ocr_hash = (
                        image_ocr_hash_file_path.read_bytes()
                    )
                except OSError:
                    pass
                else:
                    if (
                        calculate_file_hash(image_ocr_file)
                        == expected_image_ocr_hash
                    ):
                        return
        image_ocr_result = json.dumps(recognizer(image_file.read()))
        image_ocr_file_path.write_text(image_ocr_result, encoding=encoding)
        image_ocr_hash_file_path.write_bytes(
            to_hasher(image_ocr_result.encode(encoding)).digest()
        )
