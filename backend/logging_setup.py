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
    return os.path.join(log_dir, "tendersmart.txt")


def _launcher_owns_combined_log() -> bool:
    return os.getenv("TENDERSMART_LOG_OWNER", "").strip().lower() == "launcher"


def _root_log_level() -> int:
    level_name = os.getenv("TENDERSMART_ROOT_LOG_LEVEL", "INFO").strip().upper()
    return getattr(logging, level_name, logging.INFO)


def _stdout_log_level() -> int:
    level_name = os.getenv("TENDERSMART_STDOUT_LOG_LEVEL", "INFO").strip().upper()
    return getattr(logging, level_name, logging.INFO)


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
    root.setLevel(_root_log_level())

    # Жёстко очищаем старые handlers, чтобы не было дублирования.
    for handler in list(root.handlers):
        root.removeHandler(handler)
        try:
            handler.close()
        except Exception:
            pass

    import sys
    console_handler = logging.StreamHandler(
        stream=open(
            sys.stdout.fileno(),
            mode="w",
            encoding="utf-8",
            errors="replace",
            closefd=False,
            buffering=1,
    )
    )
    console_handler.setLevel(_stdout_log_level())
    console_handler.setFormatter(formatter)

    root.addHandler(console_handler)
    if not _launcher_owns_combined_log():
        file_handler = RotatingFileHandler(
            _LOG_FILE_PATH,
            mode="w",  # Явно указываем 'w' для перезаписи при каждом старте приложения
            maxBytes=20 * 1024 * 1024,
            backupCount=10,
            encoding="utf-8",
        )
        file_handler.setLevel(_root_log_level())
        file_handler.setFormatter(formatter)
        root.addHandler(file_handler)

    # Все важные логгеры должны идти в root.
    logger_levels = {
        "LegalAI": logging.INFO,
        "Frontend": logging.INFO,
        "EIS_Service": logging.INFO,
        "GidroizolParser": logging.INFO,
        "uvicorn": logging.INFO,
        "uvicorn.error": logging.INFO,
        "uvicorn.access": logging.WARNING,
        "fastapi": logging.INFO,
        "asyncio": logging.WARNING,
        "sqlalchemy": logging.WARNING,
        "sqlalchemy.engine": logging.WARNING,
        "sqlalchemy.pool": logging.WARNING,
        "sqlalchemy.orm": logging.WARNING,
        "sqlalchemy.orm.mapper": logging.WARNING,
        "sqlalchemy.orm.path_registry": logging.WARNING,
        "google": logging.WARNING,
        "httpx": logging.WARNING,
        "httpcore": logging.WARNING,
        "urllib3": logging.WARNING,
        "playwright": logging.WARNING,
        "PIL": logging.WARNING,
        "PIL.Image": logging.WARNING,
        "watchfiles": logging.WARNING,
        "py.warnings": logging.WARNING,
    }
    for name, level in logger_levels.items():
        lg = logging.getLogger(name)
        lg.handlers.clear()
        lg.propagate = True
        lg.setLevel(level)

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
    if _launcher_owns_combined_log():
        logging.getLogger("LegalAI").info("Combined log file is managed by launcher stdout capture.")


def setup_logging(name: str = "LegalAI") -> logging.Logger:
    _configure_root_logger()
    return logging.getLogger(name)


def get_logger(name: str = "LegalAI") -> logging.Logger:
    return setup_logging(name)
