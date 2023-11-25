from __future__ import annotations

from logging import Logger, getLogger
from typing import Any

import attr


@attr.s(slots=True, frozen=True, kw_only=True)
class LoggerWithTrace:
    logger: Logger = attr.ib()

    @classmethod
    def get(cls, name: str) -> LoggerWithTrace:
        return LoggerWithTrace(logger=getLogger(name))

    def info(self, *args: Any, **kwargs: Any) -> None:
        self.logger.info(*args, **kwargs)

    def warning(self, *args: Any, **kwargs: Any) -> None:
        self.logger.warning(*args, **kwargs)

    def debug(self, *args: Any, **kwargs: Any) -> None:
        self.logger.debug(*args, **kwargs)

    def error(self, *args: Any, **kwargs: Any) -> None:
        self.logger.error(*args, **kwargs)

    def critical(self, *args: Any, **kwargs: Any) -> None:
        self.logger.critical(*args, **kwargs)

    def trace(self, message: str, *args: Any, **kws: Any) -> None:
        self.logger.log(5, message, *args, **kws)
