from backend.logging_setup import setup_logging

logger = setup_logging()
logger.info("Starting backend via run_backend.py")

import uvicorn  # noqa: E402

if __name__ == "__main__":
    logger.info("Launching uvicorn backend.main:app on 0.0.0.0:8000")
    uvicorn.run("backend.main:app", host="0.0.0.0", port=8000, reload=False)
