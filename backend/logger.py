# backend/logger.py
import json
from datetime import datetime
from backend.logging_setup import setup_logging

logger = setup_logging("LegalAI")


def setup_unified_logger():
    return logger


def log_debug_event(event_data: dict):
    payload = dict(event_data or {})
    payload["timestamp"] = datetime.utcnow().isoformat() + "Z"
    try:
        logger.debug("[DEBUG_EVENT] %s", json.dumps(payload, ensure_ascii=False))
    except Exception as e:
        logger.error("Failed to write debug event: %s", e, exc_info=True)
