import json
from pathlib import Path

import easyocr  # type: ignore[import-untyped]

from .caching import calculate_file_hash, to_hasher
from .language import SupportedLanguage

_readers = {
    frozenset(languages): easyocr.Reader(languages)
    for languages in [['en'], ['en', 'ru']]
}
# we assume that ru-segment shares memes from english sources
_readers[frozenset(['ru'])] = easyocr.Reader(['en', 'ru'])


def sync_image_ocr(
    image_file_path: Path,
    /,
    *,
    encoding: str = 'utf-8',
    languages: list[SupportedLanguage],
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
        image_ocr_result = json.dumps(
            [
                [text, confidence]
                for _, text, confidence in _readers[
                    frozenset(languages)
                ].readtext(image_file.read())
            ]
        )
        image_ocr_file_path.write_text(image_ocr_result, encoding=encoding)
        image_ocr_hash_file_path.write_bytes(
            to_hasher(image_ocr_result.encode(encoding)).digest()
        )
