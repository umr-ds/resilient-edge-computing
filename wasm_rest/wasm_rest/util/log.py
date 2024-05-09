import logging
import sys


class LogFormatter(logging.Formatter):
    grey = "\x1b[38;20m"
    yellow = "\x1b[33;20m"
    green = '\033[92m'
    red = "\x1b[31;20m"
    bold_red = "\x1b[31;1m"
    reset = "\x1b[0m"
    format = "%(levelname)s: %(asctime)s: %(message)s"

    FORMATS = {
        logging.DEBUG: grey + format + reset,
        logging.INFO: green + format + reset,
        logging.WARNING: yellow + format + reset,
        logging.ERROR: red + format + reset,
        logging.CRITICAL: bold_red + format + reset
    }

    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)


LOG = logging.Logger("wasm_rest_logger")
if __name__ == 'wasm_rest.util.log':
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(LogFormatter())
    handler.setLevel(logging.DEBUG)
    LOG.addHandler(handler)
    LOG.setLevel(logging.DEBUG)
