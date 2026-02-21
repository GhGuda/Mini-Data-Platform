import os
import sys
import logging
from logging.handlers import RotatingFileHandler


def setup_logging(service_name: str) -> logging.Logger:
    """
    Configure structured logging with rotation.

    Logs are written both to file and stdout.
    """

    log_dir = "/logs"
    os.makedirs(log_dir, exist_ok=True)

    logger = logging.getLogger(service_name)
    logger.setLevel(logging.INFO)

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )

    file_handler = RotatingFileHandler(
        os.path.join(log_dir, f"{service_name}.log"),
        maxBytes=5_000_000,
        backupCount=3,
    )
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger