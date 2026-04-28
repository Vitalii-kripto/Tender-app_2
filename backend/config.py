import os
from dotenv import load_dotenv

load_dotenv()


def _env_nonempty(name: str, default: str = "") -> str:
    value = (os.getenv(name) or "").strip()
    return value or default

# Base directory of the project
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")

# Directory for storing downloaded documents
DOCUMENTS_ROOT = os.path.join(DATA_DIR, "eis_docs")

# Ensure data directories exist
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(DOCUMENTS_ROOT, exist_ok=True)

# AI Model Configuration
GEMINI_MODEL = _env_nonempty("GEMINI_MODEL", "gemini-3-flash-preview")
GEMINI_MODEL_BACKUP = _env_nonempty(
    "GEMINI_MODEL_BACKUP",
    _env_nonempty("GEMINI_FALLBACK_MODEL", "gemini-3.1-flash-lite-preview"),
)
GEMINI_MODEL_GROUND = _env_nonempty("GEMINI_MODEL_GROUND", "gemini-2.5-flash")
GEMINI_MODEL_GROUND_BACKUP = _env_nonempty(
    "GEMINI_MODEL_GROUND_BACKUP",
    _env_nonempty("GEMINI_GROUND_FALLBACK_MODEL", "gemini-2.5-flash-lite"),
)
