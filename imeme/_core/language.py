import enum


class SupportedLanguage(str, enum.Enum):
    ENGLISH = 'en'
    RUSSIAN = 'ru'

    def __str__(self, /) -> str:
        return self.value


class SupportedLanguageCategory(str, enum.Enum):
    CYRILLIC = 'cyrillic'
    LATIN = 'latin'

    @classmethod
    def is_supported(cls, value: str, /) -> bool:
        return any(value == member.value for member in cls)
