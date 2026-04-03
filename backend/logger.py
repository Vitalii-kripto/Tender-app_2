from backend.logging_setup import setup_logging, get_logger
import os
import json
from datetime import datetime

# Initialize the central logging configuration
logger = setup_logging("LegalAI")

def log_debug_event(event_data: dict):
    """
    Пишет структурированное событие в debug-лог (JSONL).
    """
    LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs')
    os.makedirs(LOG_DIR, exist_ok=True)
    DEBUG_LOG_FILE = os.path.join(LOG_DIR, 'legal_ai_debug.jsonl')
    
    event_data['timestamp'] = datetime.utcnow().isoformat() + "Z"
    try:
        with open(DEBUG_LOG_FILE, 'a', encoding='utf-8') as f:
            f.write(json.dumps(event_data, ensure_ascii=False) + '\n')
    except Exception as e:
        logger.error(f"Failed to write to debug log: {e}")
