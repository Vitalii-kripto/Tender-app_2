import uvicorn
import logging
import os

from backend.logger import logger

if __name__ == "__main__":
    logger.info("==========================================")
    logger.info("INITIATING LOCAL SERVER LAUNCH")
    logger.info("==========================================")
    try:
        # Runs the FastAPI application located in backend/main.py
        # Reload=True enables auto-reload on code changes for development
        logger.info("Starting Uvicorn process...")
        uvicorn.run("backend.main:app", host="0.0.0.0", port=8000, reload=False)
    except Exception as e:
        logger.critical(f"CRITICAL ERROR during server startup: {e}", exc_info=True)
