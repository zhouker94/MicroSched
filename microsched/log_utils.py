import logging
import os
import sys


def setup_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    """Configure and return a customized logger to avoid global root logger conflicts."""
    logger = logging.getLogger(name)

    # Avoid adding multiple handlers if the logger is requested multiple times
    if not logger.handlers:
        formatter = logging.Formatter(
            "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
        )

        # 1. Console Handler (Standard Output)
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

        # 2. File Handler (Write to logs/ folder)
        os.makedirs("logs", exist_ok=True)
        file_handler = logging.FileHandler(
            f"logs/{name}.log", encoding="utf-8")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        logger.setLevel(level)
        logger.propagate = False  # Prevent logs from bubbling up to the root logger

    return logger
