import logging
import sys
from collections.abc import Mapping
from types import MappingProxyType
from typing import ClassVar


class LogFormatter(logging.Formatter):
    grey = "\x1b[38;20m"
    yellow = "\x1b[33;20m"
    green = "\033[92m"
    red = "\x1b[31;20m"
    bold_red = "\x1b[31;1m"
    reset = "\x1b[0m"
    _format = "%(levelname)s: %(asctime)s: %(message)s"

    FORMATS: ClassVar[Mapping[int, str]] = MappingProxyType(
        {
            logging.DEBUG: grey + _format + reset,
            logging.INFO: green + _format + reset,
            logging.WARNING: yellow + _format + reset,
            logging.ERROR: red + _format + reset,
            logging.CRITICAL: bold_red + _format + reset,
        }
    )

    def format(self, record: logging.LogRecord) -> str:
        if sys.stderr.isatty():
            log_fmt = self.FORMATS.get(record.levelno)
        else:
            log_fmt = self._format
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)


LOG = logging.getLogger("rec_logger")
if __name__ == "rec.log":
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(LogFormatter())
    handler.setLevel(logging.DEBUG)
    LOG.addHandler(handler)
    LOG.setLevel(logging.DEBUG)
