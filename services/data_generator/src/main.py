import os
import sys
import uuid
import signal
import logging
from datetime import datetime
from generator import SalesDataGenerator
import pandas as pd
import threading

from logging_config import setup_logging
from uploader import MinIOUploader

# ============================================================
# Configuration
# ============================================================

RECORD_COUNT = int(os.getenv("GENERATOR_RECORD_COUNT", "100"))
OUTPUT_DIR = os.getenv("GENERATOR_OUTPUT_DIR", "/data/raw")
TIMEOUT_SECONDS = int(os.getenv("GENERATOR_TIMEOUT_SECONDS", "60"))

logger = setup_logging("data_generator")


def generate_filename() -> str:
    """
    Generate a unique CSV filename using timestamp and UUID.
    """
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    unique_id = uuid.uuid4().hex
    return f"sales_{timestamp}_{unique_id}.csv"


def validate_configuration() -> None:
    """
    Validate required configuration values.
    """
    if RECORD_COUNT <= 0:
        raise ValueError("GENERATOR_RECORD_COUNT must be greater than 0.")

    if TIMEOUT_SECONDS <= 0:
        raise ValueError("GENERATOR_TIMEOUT_SECONDS must be greater than 0.")


def write_csv_with_timeout(df: pd.DataFrame, output_path: str, timeout: int, logger: logging.Logger) -> None:
    """
    Write DataFrame to CSV with timeout protection.
    """

    def _write():
        df.to_csv(output_path, index=False)

    thread = threading.Thread(target=_write)
    thread.start()
    thread.join(timeout)

    if thread.is_alive():
        logger.error("CSV writing timed out.")
        raise TimeoutError("CSV write operation timed out.")

    logger.info(f"CSV file written successfully: {output_path}")


def main() -> None:
    """
    Entry point for data generator service.
    """
    try:
        logger.info("Starting Sales Data Generator service.")
        validate_configuration()

        filename = generate_filename()
        logger.info(f"Generated filename: {filename}")

        os.makedirs(OUTPUT_DIR, exist_ok=True)

        generator = SalesDataGenerator(RECORD_COUNT, logger)
        df = generator.generate()

        output_path = os.path.join(OUTPUT_DIR, filename)

        write_csv_with_timeout(df, output_path, TIMEOUT_SECONDS, logger)
        uploader = MinIOUploader(logger)
        uploader.upload_file(output_path)

        logger.info("Data Generator completed successfully.")
        sys.exit(0)

    except Exception:
        logger.exception("Data Generator failed.")
        sys.exit(1)


if __name__ == "__main__":
    main()