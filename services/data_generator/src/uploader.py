import os
import logging
from minio import Minio
from minio.error import S3Error
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type


class MinIOUploader:
    """
    Handles uploading files to MinIO with retry logic.
    """

    def __init__(self, logger: logging.Logger):
        self.logger = logger

        self.endpoint = os.getenv("MINIO_ENDPOINT", "minio:9000")
        self.access_key = os.getenv("MINIO_ROOT_USER")
        self.secret_key = os.getenv("MINIO_ROOT_PASSWORD")
        self.bucket = os.getenv("MINIO_BUCKET_RAW")

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=2, min=2, max=10),
        retry=retry_if_exception_type(Exception),
        reraise=True,
    )
    def connect(self) -> Minio:
        """
        Establish connection to MinIO with retry logic.
        """
        self.logger.info("Connecting to MinIO...")

        client = Minio(
            self.endpoint,
            access_key=self.access_key,
            secret_key=self.secret_key,
            secure=False,
        )

        # Validate connection
        client.list_buckets()

        self.logger.info("Connected to MinIO successfully.")
        return client

    def upload_file(self, file_path: str) -> None:
        """
        Upload file to MinIO bucket.
        """
        try:
            client = self.connect()

            object_name = os.path.basename(file_path)

            if not client.bucket_exists(self.bucket):
                raise ValueError(f"Bucket does not exist: {self.bucket}")

            self.logger.info(f"Uploading file to bucket {self.bucket}: {object_name}")

            client.fput_object(
                self.bucket,
                object_name,
                file_path,
            )

            self.logger.info("File uploaded successfully.")

        except S3Error:
            self.logger.exception("MinIO S3 error occurred during upload.")
            raise
        except Exception:
            self.logger.exception("Unexpected error during upload.")
            raise