# backend/logging_setup.py

import logging
import os
import sys
from logging.handlers import RotatingFileHandler

_unified_logger_configured = False

def setup_logging(name: str = "LegalAI") -> logging.Logger:
    global _unified_logger_configured
    logger = logging.getLogger(name)

    if _unified_logger_configured:
        return logger

    _unified_logger_configured = True

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(filename)s:%(lineno)d | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    console = logging.StreamHandler(sys.stdout)
    console.setLevel(logging.INFO)
    console.setFormatter(formatter)

    try:
        log_dir = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "logs"
        )
        os.makedirs(log_dir, exist_ok=True)
        log_file = os.path.join(log_dir, "tendersmart.log")

        file_handler = RotatingFileHandler(
            log_file,
            maxBytes=10 * 1024 * 1024,  # 10 MB
            backupCount=5,
            encoding="utf-8",
        )
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(formatter)
    except Exception as e:
        print(f"Не удалось создать файловый лог: {e}")
        file_handler = None

    # Configure root logger and specific loggers
    loggers_to_configure = [
        logging.getLogger(),  # root
        logging.getLogger("LegalAI"),
        logging.getLogger("Frontend"),
        logging.getLogger("uvicorn"),
        logging.getLogger("uvicorn.error"),
        logging.getLogger("uvicorn.access"),
        logging.getLogger("fastapi"),
        logging.getLogger("asyncio"),
        logging.getLogger("py.warnings"),
    ]

    for l in loggers_to_configure:
        l.handlers.clear()
        l.setLevel(logging.INFO)
        l.addHandler(console)
        if file_handler:
            l.addHandler(file_handler)
        l.propagate = False

    logging.captureWarnings(True)

    def handle_exception(exc_type, exc_value, exc_traceback):
        if issubclass(exc_type, KeyboardInterrupt):
            sys.__excepthook__(exc_type, exc_value, exc_traceback)
            return
        logging.getLogger("LegalAI").error("Uncaught exception", exc_info=(exc_type, exc_value, exc_traceback))

    sys.excepthook = handle_exception

    logger.info("--- [UNIFIED LOGGER INITIALIZED] ---")
    return logger

def get_logger(name: str = "LegalAI") -> logging.Logger:
    return logging.getLogger(name)
