# backend/logging_setup.py

import logging
import os
from logging.handlers import RotatingFileHandler


def setup_logging(name: str = "LegalAI") -> logging.Logger:
    """
    Настраивает и возвращает логгер приложения.
    Пишет одновременно в консоль и в файл logs/tendersmart.log
    с автоматической ротацией (макс. 10 МБ, 5 архивов).
    """
    logger = logging.getLogger(name)

    # Если уже настроен — не дублируем хендлеры
    if logger.handlers:
        return logger

    logger.setLevel(logging.DEBUG)

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(filename)s:%(lineno)d | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # --- Хендлер: консоль ---
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(formatter)
    logger.addHandler(console)

    # --- Хендлер: файл с ротацией ---
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
        logger.addHandler(file_handler)
    except Exception as e:
        logger.warning(f"Не удалось создать файловый лог: {e}")

    logger.info("--- [UNIFIED LOGGER INITIALIZED] ---")
    return logger
