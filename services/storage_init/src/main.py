import os
import sys
import logging
from logging.handlers import RotatingFileHandler
from minio import Minio
from minio.error import S3Error
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from logging_config import setup_logging
# ============================================================
# Configuration
# ============================================================

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ROOT_USER = os.getenv("MINIO_ROOT_USER")
MINIO_ROOT_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD")

BUCKET_RAW = os.getenv("MINIO_BUCKET_RAW")
BUCKET_PROCESSED = os.getenv("MINIO_BUCKET_PROCESSED")

LOG_DIR = "/logs" 
LOG_FILE = os.path.join(LOG_DIR, "storage_init.log")

# ============================================================
# Logging Setup
# ============================================================
logger = setup_logging("storage_init")


# ============================================================
# MinIO Connection with Retry
# ============================================================

@retry(
    stop=stop_after_attempt(10),
    wait=wait_exponential(multiplier=2, min=2, max=10),
    retry=retry_if_exception_type(Exception),
)
def connect_minio() -> Minio:
    """
    Attempt to establish a connection to MinIO with retry logic.
    """
    logger.info("Attempting to connect to MinIO...")

    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ROOT_USER,
        secret_key=MINIO_ROOT_PASSWORD,
        secure=False
    )

    # Test connection by listing buckets
    client.list_buckets()

    logger.info("Successfully connected to MinIO.")
    return client


def ensure_bucket_exists(client: Minio, bucket_name: str) -> None:
    """
    Create bucket if it does not exist.
    """
    if not bucket_name:
        raise ValueError("Bucket name cannot be empty.")

    if not client.bucket_exists(bucket_name):
        logger.info(f"Creating bucket: {bucket_name}")
        client.make_bucket(bucket_name)
    else:
        logger.info(f"Bucket already exists: {bucket_name}")


def main() -> None:
    """
    Entry point for storage initialization service.
    """
    try:
        client = connect_minio()

        ensure_bucket_exists(client, BUCKET_RAW)
        ensure_bucket_exists(client, BUCKET_PROCESSED)

        logger.info("Storage initialization completed successfully.")
        sys.exit(0)

    except Exception as e:
        logger.exception("Storage initialization failed.")
        sys.exit(1)


if __name__ == "__main__":
    main()