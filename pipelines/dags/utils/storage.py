"""
MinIO Storage Utility Module
Handles object listing and file movement operations.
"""

import os
import logging
from typing import List

from minio import Minio
from minio.commonconfig import CopySource
from minio.error import S3Error
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type


class MinIOStorage:
    """
    Handles interactions with MinIO object storage.
    """

    def __init__(self, logger: logging.Logger):
        self.logger = logger

        self.endpoint = os.getenv("MINIO_ENDPOINT", "minio:9000")
        self.access_key = os.getenv("MINIO_ROOT_USER")
        self.secret_key = os.getenv("MINIO_ROOT_PASSWORD")

        self.raw_bucket = os.getenv("MINIO_BUCKET_RAW")
        self.processed_bucket = os.getenv("MINIO_BUCKET_PROCESSED")

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
        self.logger.info("Connecting to MinIO for file listing...")

        client = Minio(
            self.endpoint,
            access_key=self.access_key,
            secret_key=self.secret_key,
            secure=False,
        )

        client.list_buckets()  # Validate connection

        self.logger.info("Connected to MinIO successfully.")
        return client

    def list_raw_csv_files(self) -> List[str]:
        """
        List all CSV files in the raw bucket.
        """
        try:
            client = self.connect()
            if not self.raw_bucket:
                raise ValueError("MINIO_BUCKET_RAW is not configured.")
            if not client.bucket_exists(self.raw_bucket):
                self.logger.warning(
                    f"Raw bucket does not exist yet: {self.raw_bucket}"
                )
                return []

            objects = client.list_objects(self.raw_bucket, recursive=True)

            files = [
                obj.object_name
                for obj in objects
                if obj.object_name.endswith(".csv")
            ]

            self.logger.info(f"Found {len(files)} raw CSV file(s).")

            return files

        except S3Error:
            self.logger.exception("MinIO S3 error while listing files.")
            raise
        except Exception:
            self.logger.exception("Unexpected error during file listing.")
            raise
        
    
    def download_file(self, object_name: str, download_dir: str = "/tmp") -> str:
        """
        Downloads a file from MinIO raw bucket to a local temporary directory.

        Returns the local file path.
        """
        try:
            client = self.connect()

            os.makedirs(download_dir, exist_ok=True)

            local_path = os.path.join(download_dir, object_name)

            self.logger.info(f"Downloading file: {object_name}")

            client.fget_object(
                self.raw_bucket,
                object_name,
                local_path,
            )

            self.logger.info(f"File downloaded successfully: {local_path}")

            return local_path

        except Exception:
            self.logger.exception("Error downloading file from MinIO.")
            raise
        
        
    def move_to_processed(self, object_name: str):
        """
        Move file from raw bucket to processed bucket.
        """

        try:
            client = self.connect()
            if not self.raw_bucket:
                raise ValueError("MINIO_BUCKET_RAW is not configured.")
            if not self.processed_bucket:
                raise ValueError("MINIO_BUCKET_PROCESSED is not configured.")
            if not client.bucket_exists(self.processed_bucket):
                client.make_bucket(self.processed_bucket)

            self.logger.info(f"Moving file to processed bucket: {object_name}")

            source = CopySource(self.raw_bucket, object_name)

            # Copy object
            client.copy_object(
                self.processed_bucket,
                object_name,
                source,
            )

            # Remove original
            client.remove_object(self.raw_bucket, object_name)

            self.logger.info(f"File moved successfully: {object_name}")

        except Exception:
            self.logger.exception("Error moving file to processed bucket.")
            raise
