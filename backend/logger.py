import os
import sys
import json
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOG_DIR = os.path.join(BASE_DIR, "logs")
os.makedirs(LOG_DIR, exist_ok=True)

LOG_FILE = os.path.join(LOG_DIR, "tendersmart.log")
DEBUG_LOG_FILE = os.path.join(LOG_DIR, "legal_ai_debug.jsonl")


class SafeStreamHandler(logging.StreamHandler):
    def emit(self, record):
        try:
            super().emit(record)
        except UnicodeEncodeError:
            try:
                msg = self.format(record)
                msg = msg.replace("₽", "руб.")
                stream = self.stream
                stream.write(msg + self.terminator)
                self.flush()
            except Exception:
                self.handleError(record)


def configure_logging():
    if getattr(configure_logging, "_configured", False):
        return logging.getLogger("LegalAI")

    os.makedirs(LOG_DIR, exist_ok=True)

    root = logging.getLogger()
    root.setLevel(logging.INFO)

    for h in list(root.handlers):
        root.removeHandler(h)

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(filename)s:%(lineno)d | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    file_handler = RotatingFileHandler(
        LOG_FILE,
        maxBytes=20 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8",
    )
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)

    try:
        if hasattr(sys.stdout, "reconfigure"):
            sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

    console_handler = SafeStreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)

    root.addHandler(file_handler)
    root.addHandler(console_handler)

    for name in [
        "LegalAI",
        "EIS_Service",
        "Frontend",
        "uvicorn",
        "uvicorn.error",
        "uvicorn.access",
        "fastapi",
        "asyncio",
    ]:
        lgr = logging.getLogger(name)
        lgr.setLevel(logging.INFO)
        lgr.propagate = True
        for h in list(lgr.handlers):
            lgr.removeHandler(h)

    configure_logging._configured = True

    logger = logging.getLogger("LegalAI")
    logger.info("--- [UNIFIED LOGGER INITIALIZED] ---")
    logger.info("Unified log file: %s", LOG_FILE)
    return logger


def log_debug_event(event_data: dict):
    event_data["timestamp"] = datetime.utcnow().isoformat() + "Z"
    try:
        with open(DEBUG_LOG_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(event_data, ensure_ascii=False) + "\n")
    except Exception as e:
        logging.getLogger("LegalAI").error("Failed to write debug log: %s", e)


logger = configure_logging()
