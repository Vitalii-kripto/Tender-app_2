import io
import logging
import os
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path

LOG_DIR = Path("logs")
LOG_DIR.mkdir(parents=True, exist_ok=True)

APP_LOG_FILE = LOG_DIR / "app.log"

_LOG_FORMAT = (
    "%(asctime)s | %(levelname)s | %(name)s | "
    "%(filename)s:%(lineno)d | %(message)s"
)

_configured = False


class StreamToLogger(io.TextIOBase):
    def __init__(self, logger: logging.Logger, level: int):
        super().__init__()
        self.logger = logger
        self.level = level
        self._buffer = ""

    def write(self, message):
        if not message:
            return 0
        self._buffer += message
        while "\n" in self._buffer:
            line, self._buffer = self._buffer.split("\n", 1)
            line = line.strip()
            if line:
                self.logger.log(self.level, line)
        return len(message)

    def flush(self):
        if self._buffer.strip():
            self.logger.log(self.level, self._buffer.strip())
        self._buffer = ""


def setup_logging() -> logging.Logger:
    global _configured
    if _configured:
        return logging.getLogger("TenderApp")

    root = logging.getLogger()
    root.setLevel(logging.INFO)

    for handler in list(root.handlers):
        root.removeHandler(handler)

    formatter = logging.Formatter(_LOG_FORMAT)

    console_handler = logging.StreamHandler(sys.__stdout__)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)

    file_handler = RotatingFileHandler(
        APP_LOG_FILE,
        maxBytes=10 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8",
    )
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)

    root.addHandler(console_handler)
    root.addHandler(file_handler)

    logging.captureWarnings(True)

    stdout_logger = logging.getLogger("STDOUT")
    stderr_logger = logging.getLogger("STDERR")

    sys.stdout = StreamToLogger(stdout_logger, logging.INFO)
    sys.stderr = StreamToLogger(stderr_logger, logging.ERROR)

    _configured = True
    app_logger = logging.getLogger("TenderApp")
    app_logger.info("Unified logging configured. Main log file: %s", APP_LOG_FILE)
    return app_logger
