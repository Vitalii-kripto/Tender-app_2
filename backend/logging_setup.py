# backend/logging_setup.py
import logging
import os
import sys
import threading
from logging.handlers import RotatingFileHandler
from typing import Optional

_LOGGING_CONFIGURED = False
_LOG_FILE_PATH: Optional[str] = None


def _project_root() -> str:
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _build_log_file_path() -> str:
    log_dir = os.path.join(_project_root(), "logs")
    os.makedirs(log_dir, exist_ok=True)
    return os.path.join(log_dir, "tendersmart.log")


def _configure_root_logger() -> None:
    global _LOGGING_CONFIGURED, _LOG_FILE_PATH
    if _LOGGING_CONFIGURED:
        return

    _LOG_FILE_PATH = _build_log_file_path()

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(filename)s:%(lineno)d | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    root = logging.getLogger()
    root.setLevel(logging.DEBUG)

    # Жёстко очищаем старые handlers, чтобы не было дублирования.
    for handler in list(root.handlers):
        root.removeHandler(handler)
        try:
            handler.close()
        except Exception:
            pass

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)

    file_handler = RotatingFileHandler(
        _LOG_FILE_PATH,
        maxBytes=10 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8",
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)

    root.addHandler(console_handler)
    root.addHandler(file_handler)

    # Все важные логгеры должны идти в root.
    important_loggers = [
        "LegalAI",
        "Frontend",
        "EIS_Service",
        "uvicorn",
        "uvicorn.error",
        "uvicorn.access",
        "fastapi",
        "asyncio",
        "sqlalchemy",
        "sqlalchemy.engine",
    ]
    for name in important_loggers:
        lg = logging.getLogger(name)
        lg.handlers.clear()
        lg.propagate = True
        if name in {"uvicorn.access"}:
            lg.setLevel(logging.INFO)
        else:
            lg.setLevel(logging.DEBUG)

    logging.captureWarnings(True)

    def _handle_exception(exc_type, exc_value, exc_traceback):
        if issubclass(exc_type, KeyboardInterrupt):
            sys.__excepthook__(exc_type, exc_value, exc_traceback)
            return
        logging.getLogger("LegalAI").critical(
            "Unhandled exception",
            exc_info=(exc_type, exc_value, exc_traceback),
        )

    def _handle_thread_exception(args):
        logging.getLogger("LegalAI").critical(
            "Unhandled thread exception",
            exc_info=(args.exc_type, args.exc_value, args.exc_traceback),
        )

    sys.excepthook = _handle_exception
    if hasattr(threading, "excepthook"):
        threading.excepthook = _handle_thread_exception

    _LOGGING_CONFIGURED = True
    logging.getLogger("LegalAI").info("--- [UNIFIED LOGGER INITIALIZED] ---")
    logging.getLogger("LegalAI").info(f"Unified log file: {_LOG_FILE_PATH}")


def setup_logging(name: str = "LegalAI") -> logging.Logger:
    _configure_root_logger()
    return logging.getLogger(name)


def get_logger(name: str = "LegalAI") -> logging.Logger:
    return setup_logging(name)
