import os

# Base directory of the project
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")

# Directory for storing downloaded documents
DOCUMENTS_ROOT = os.path.join(DATA_DIR, "eis_docs")

# Ensure data directories exist
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(DOCUMENTS_ROOT, exist_ok=True)

# AI Model Configuration
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-3-flash-preview")
GEMINI_FALLBACK_MODEL = os.getenv("GEMINI_FALLBACK_MODEL", "gemini-3.1-flash-lite-preview")
