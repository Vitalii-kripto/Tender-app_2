import os
import logging
import sys
import json
from datetime import datetime

# Путь к общему лог-файлу
LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs')
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, 'legal_ai.log')
DEBUG_LOG_FILE = os.path.join(LOG_DIR, 'legal_ai_debug.jsonl')

def setup_unified_logger():
    """
    Настраивает единый логгер для всех сервисов анализа.
    Пишет в legal_ai.log в режиме append.
    """
    logger = logging.getLogger("LegalAI")
    
    # Если логгер уже настроен, не добавляем хендлеры повторно
    if logger.hasHandlers():
        return logger

    logger.setLevel(logging.INFO)
    
    # Формат логов
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    # Хендлер для файла (режим 'w' - overwrite)
    file_handler = logging.FileHandler(LOG_FILE, encoding='utf-8', mode='w')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    # Хендлер для фронтенд-логов
    frontend_log_file = os.path.join(LOG_DIR, 'frontend.log')
    frontend_handler = logging.FileHandler(frontend_log_file, encoding='utf-8', mode='w')
    frontend_handler.setFormatter(formatter)
    frontend_logger = logging.getLogger("Frontend")
    frontend_logger.setLevel(logging.INFO)
    frontend_logger.addHandler(frontend_handler)
    frontend_logger.propagate = False
    
    # Хендлер для консоли
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)
    
    # Запрещаем передачу логов корневому логгеру, чтобы избежать дублирования
    logger.propagate = False
    
    logger.info("--- [UNIFIED LOGGER INITIALIZED] ---")
    return logger

def log_debug_event(event_data: dict):
    """
    Пишет структурированное событие в debug-лог (JSONL).
    """
    event_data['timestamp'] = datetime.utcnow().isoformat() + "Z"
    try:
        with open(DEBUG_LOG_FILE, 'a', encoding='utf-8') as f:
            f.write(json.dumps(event_data, ensure_ascii=False) + '\n')
    except Exception as e:
        logger.error(f"Failed to write to debug log: {e}")

# Создаем экземпляр логгера для импорта
logger = setup_unified_logger()
