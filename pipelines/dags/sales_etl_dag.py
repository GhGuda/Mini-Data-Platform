"""
Sales ETL DAG

Processes raw sales CSV files from MinIO,
transforms them, loads into PostgreSQL,
and moves processed files.
"""

from datetime import datetime, timedelta
import logging
from utils.storage import MinIOStorage

from airflow import DAG
from airflow.operators.python import PythonOperator
from utils.transformer import SalesTransformer
from utils.load_db import PostgresLoader

# ============================================================
# Default DAG Arguments
# ============================================================

default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
}

# ============================================================
# DAG Definition
# ============================================================

with DAG(
    dag_id="sales_etl_pipeline",
    default_args=default_args,
    description="ETL pipeline for processing sales data from MinIO",
    schedule_interval="*/5 * * * *",  # Every 5 minutes
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["sales", "etl"],
) as dag:

    def list_raw_files(**context):
        """
        Lists all raw CSV files from MinIO and pushes them to XCom.
        """

        logger = logging.getLogger("airflow.task")

        storage = MinIOStorage(logger)
        files = storage.list_raw_csv_files()

        logger.info(f"Discovered {len(files)} file(s) in raw bucket.")

        context["ti"].xcom_push(key="raw_files", value=files)

    def process_files(**context):
        """
        Downloads all discovered raw files for processing.
        """

        logger = logging.getLogger("airflow.task")

        files = context["ti"].xcom_pull(key="raw_files", task_ids="list_raw_files")

        if not files:
            logger.info("No files to process.")
            return

        storage = MinIOStorage(logger)

        transformer = SalesTransformer(logger)

        for file_name in files:
            logger.info(f"Processing file: {file_name}")

            local_path = storage.download_file(file_name)

            raw_df = transformer.load_raw(local_path)

            normalized_data = transformer.transform(raw_df)
            loader = PostgresLoader(logger)
            loader.load(normalized_data)
            storage.move_to_processed(file_name)

            logger.info(
                f"Customers: {len(normalized_data['customers'])}, "
                f"Products: {len(normalized_data['products'])}, "
                f"Order lines: {len(normalized_data['order_lines'])}"
            )

    def finalize(**context):
        """
        Placeholder final task.
        """
        pass

    list_task = PythonOperator(
        task_id="list_raw_files",
        python_callable=list_raw_files,
        provide_context=True,
    )

    process_task = PythonOperator(
        task_id="process_files",
        python_callable=process_files,
        provide_context=True,
    )

    finalize_task = PythonOperator(
        task_id="finalize",
        python_callable=finalize,
    )

    list_task >> process_task >> finalize_task
