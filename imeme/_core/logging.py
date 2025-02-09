import logging


class LevelRangeFilter(logging.Filter):
    def __init__(
        self,
        *,
        min_level: int | str | None = None,
        max_level: int | str | None = None,
    ) -> None:
        super().__init__()
        normalized_min_level = (
            logging.getLevelName(min_level)
            if isinstance(min_level, str)
            else min_level
        )
        normalized_max_level = (
            logging.getLevelName(max_level)
            if isinstance(max_level, str)
            else max_level
        )
        assert (
            normalized_max_level is not None
            if normalized_min_level is None
            else (
                normalized_max_level is None
                or normalized_min_level <= normalized_max_level
            )
        ), (min_level, max_level)
        self._max_level, self._min_level = (
            normalized_max_level,
            normalized_min_level,
        )

    def filter(self, record: logging.LogRecord) -> bool:
        return (
            self._min_level is None or self._min_level <= record.levelno
        ) and (self._max_level is None or record.levelno <= self._max_level)
