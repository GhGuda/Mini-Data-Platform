from __future__ import annotations

import io
import os
import subprocess
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

import pytest
import requests
from dotenv import dotenv_values
from minio import Minio
from minio.error import S3Error

try:
    import psycopg
except ImportError:  # pragma: no cover - fallback for older local environments
    import psycopg2 as psycopg


REPO_ROOT = Path(__file__).resolve().parents[2]


@dataclass(frozen=True)
class TestConfig:
    airflow_container: str
    dag_id: str
    minio_endpoint: str
    minio_access_key: str
    minio_secret_key: str
    minio_raw_bucket: str
    minio_processed_bucket: str
    postgres_host: str
    postgres_port: int
    postgres_db: str
    postgres_user: str
    postgres_password: str
    metabase_url: str
    metabase_admin_email: str
    metabase_admin_password: str
    metabase_admin_first_name: str
    metabase_admin_last_name: str
    metabase_postgres_host: str
    metabase_postgres_port: int
    metabase_database_name: str


@dataclass(frozen=True)
class PipelineRunResult:
    object_name: str
    order_code: str
    fact_sales_rows: int


def _run_command(
    args: list[str],
    *,
    timeout_seconds: int = 180,
) -> str:
    result = subprocess.run(
        args,
        cwd=REPO_ROOT,
        text=True,
        capture_output=True,
        timeout=timeout_seconds,
    )
    if result.returncode != 0:
        joined_args = " ".join(args)
        raise AssertionError(
            f"Command failed: {joined_args}\n"
            f"stdout:\n{result.stdout}\n"
            f"stderr:\n{result.stderr}"
        )
    return result.stdout.strip()


def _wait_until(
    description: str,
    predicate: Callable[[], bool],
    *,
    timeout_seconds: int = 180,
    poll_interval_seconds: int = 3,
) -> None:
    deadline = time.time() + timeout_seconds
    last_error: Exception | None = None

    while time.time() < deadline:
        try:
            if predicate():
                return
        except Exception as exc:  # pragma: no cover - error detail path
            last_error = exc
        time.sleep(poll_interval_seconds)

    if last_error:
        raise AssertionError(
            f"Timed out waiting for {description}. Last error: {last_error}"
        ) from last_error
    raise AssertionError(f"Timed out waiting for {description}.")


def _load_test_config() -> TestConfig:
    dotenv_path = REPO_ROOT / "docker" / ".env"
    docker_env = dotenv_values(dotenv_path) if dotenv_path.exists() else {}

    def pick(name: str, default: str | None = None) -> str:
        value = os.getenv(name)
        if value:
            return value
        docker_value = docker_env.get(name)
        if docker_value:
            return str(docker_value)
        if default is not None:
            return default
        raise AssertionError(
            f"Missing required configuration value: {name}. "
            "Set it in docker/.env or process environment."
        )

    return TestConfig(
        airflow_container=pick("TEST_AIRFLOW_CONTAINER", "mini-airflow-web"),
        dag_id=pick("TEST_AIRFLOW_DAG_ID", "sales_etl_pipeline"),
        minio_endpoint=pick("TEST_MINIO_ENDPOINT", "localhost:9000"),
        minio_access_key=pick("MINIO_ROOT_USER"),
        minio_secret_key=pick("MINIO_ROOT_PASSWORD"),
        minio_raw_bucket=pick("MINIO_BUCKET_RAW", "sales-raw"),
        minio_processed_bucket=pick("MINIO_BUCKET_PROCESSED", "sales-processed"),
        postgres_host=pick("TEST_POSTGRES_HOST", "localhost"),
        postgres_port=int(pick("TEST_POSTGRES_PORT", pick("POSTGRES_PORT", "5432"))),
        postgres_db=pick("POSTGRES_DB", "sales_db"),
        postgres_user=pick("POSTGRES_USER"),
        postgres_password=pick("POSTGRES_PASSWORD"),
        metabase_url=pick("TEST_METABASE_URL", "http://localhost:3000").rstrip("/"),
        metabase_admin_email=pick(
            "TEST_METABASE_ADMIN_EMAIL",
            "admin@mini-data-platform.local",
        ),
        metabase_admin_password=pick(
            "TEST_METABASE_ADMIN_PASSWORD",
            "MiniDataPlatform123!",
        ),
        metabase_admin_first_name=pick("TEST_METABASE_ADMIN_FIRST_NAME", "Mini"),
        metabase_admin_last_name=pick("TEST_METABASE_ADMIN_LAST_NAME", "Platform"),
        metabase_postgres_host=pick("TEST_METABASE_POSTGRES_HOST", "postgres"),
        metabase_postgres_port=int(pick("TEST_METABASE_POSTGRES_PORT", "5432")),
        metabase_database_name=pick(
            "TEST_METABASE_DATABASE_NAME",
            "Sales Warehouse",
        ),
    )


def _build_minio_client(config: TestConfig) -> Minio:
    return Minio(
        config.minio_endpoint,
        access_key=config.minio_access_key,
        secret_key=config.minio_secret_key,
        secure=False,
    )


def _object_exists(client: Minio, bucket_name: str, object_name: str) -> bool:
    try:
        client.stat_object(bucket_name, object_name)
        return True
    except S3Error as exc:
        if exc.code in {"NoSuchKey", "NoSuchObject", "NoSuchBucket"}:
            return False
        raise


def _can_connect_postgres(config: TestConfig) -> bool:
    with psycopg.connect(
        host=config.postgres_host,
        port=config.postgres_port,
        dbname=config.postgres_db,
        user=config.postgres_user,
        password=config.postgres_password,
    ) as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT 1;")
            return cursor.fetchone() == (1,)


def _metabase_health_ok(config: TestConfig) -> bool:
    response = requests.get(
        f"{config.metabase_url}/api/health",
        timeout=10,
    )
    if response.status_code != 200:
        return False
    payload = response.json()
    return payload.get("status") == "ok"


def _airflow_cli_ready(config: TestConfig) -> bool:
    result = subprocess.run(
        [
            "docker",
            "exec",
            config.airflow_container,
            "airflow",
            "dags",
            "list",
        ],
        cwd=REPO_ROOT,
        text=True,
        capture_output=True,
        timeout=120,
    )
    return result.returncode == 0 and config.dag_id in result.stdout


def _fact_sales_count_for_order(config: TestConfig, order_code: str) -> int:
    with psycopg.connect(
        host=config.postgres_host,
        port=config.postgres_port,
        dbname=config.postgres_db,
        user=config.postgres_user,
        password=config.postgres_password,
    ) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'public'
                  AND table_name = 'fact_sales';
                """
            )
            fact_sales_columns = {row[0] for row in cursor.fetchall()}

            if "order_code" in fact_sales_columns:
                cursor.execute(
                    """
                    SELECT COUNT(*)
                    FROM fact_sales
                    WHERE order_code = %s;
                    """,
                    (order_code,),
                )
            else:
                cursor.execute(
                    """
                    SELECT COUNT(*)
                    FROM fact_sales fs
                    JOIN orders o ON fs.order_id = o.id
                    WHERE o.order_code = %s;
                    """,
                    (order_code,),
                )

            return int(cursor.fetchone()[0])


def _ensure_metabase_database(config: TestConfig) -> tuple[str, int]:
    properties_response = requests.get(
        f"{config.metabase_url}/api/session/properties",
        timeout=20,
    )
    properties_response.raise_for_status()
    setup_token = properties_response.json().get("setup-token")

    if setup_token:
        setup_payload = {
            "token": setup_token,
            "prefs": {"site_name": "Mini Data Platform"},
            "user": {
                "email": config.metabase_admin_email,
                "password": config.metabase_admin_password,
                "first_name": config.metabase_admin_first_name,
                "last_name": config.metabase_admin_last_name,
            },
            "database": {
                "name": config.metabase_database_name,
                "engine": "postgres",
                "is_full_sync": True,
                "is_sample": False,
                "details": {
                    "host": config.metabase_postgres_host,
                    "port": config.metabase_postgres_port,
                    "dbname": config.postgres_db,
                    "user": config.postgres_user,
                    "password": config.postgres_password,
                    "ssl": False,
                },
            },
        }
        setup_response = requests.post(
            f"{config.metabase_url}/api/setup",
            json=setup_payload,
            timeout=30,
        )
        setup_response.raise_for_status()

    login_response = requests.post(
        f"{config.metabase_url}/api/session",
        json={
            "username": config.metabase_admin_email,
            "password": config.metabase_admin_password,
        },
        timeout=20,
    )
    if login_response.status_code != 200:
        raise AssertionError(
            "Unable to log in to Metabase. "
            "If Metabase is already configured with different credentials, set "
            "TEST_METABASE_ADMIN_EMAIL and TEST_METABASE_ADMIN_PASSWORD for the test run."
        )
    session_id = login_response.json()["id"]
    headers = {"X-Metabase-Session": session_id}

    database_response = requests.get(
        f"{config.metabase_url}/api/database",
        headers=headers,
        timeout=20,
    )
    database_response.raise_for_status()
    databases = database_response.json().get("data", [])

    for database in databases:
        details = database.get("details") or {}
        if (
            database.get("engine") == "postgres"
            and details.get("dbname") == config.postgres_db
        ):
            return session_id, int(database["id"])

    create_response = requests.post(
        f"{config.metabase_url}/api/database",
        headers=headers,
        json={
            "name": config.metabase_database_name,
            "engine": "postgres",
            "is_full_sync": True,
            "is_sample": False,
            "details": {
                "host": config.metabase_postgres_host,
                "port": config.metabase_postgres_port,
                "dbname": config.postgres_db,
                "user": config.postgres_user,
                "password": config.postgres_password,
                "ssl": False,
            },
        },
        timeout=30,
    )
    create_response.raise_for_status()
    created = create_response.json()
    return session_id, int(created["id"])


def _query_order_count_via_metabase(
    config: TestConfig,
    *,
    session_id: str,
    database_id: int,
    order_code: str,
) -> int:
    escaped_code = order_code.replace("'", "''")
    query = (
        "SELECT COUNT(*) AS row_count "
        "FROM fact_sales fs "
        "JOIN orders o ON fs.order_id = o.id "
        f"WHERE o.order_code = '{escaped_code}';"
    )
    response = requests.post(
        f"{config.metabase_url}/api/dataset",
        headers={"X-Metabase-Session": session_id},
        json={
            "database": database_id,
            "type": "native",
            "native": {"query": query},
            "parameters": [],
        },
        timeout=30,
    )
    response.raise_for_status()
    rows = response.json().get("data", {}).get("rows", [])
    if not rows:
        return 0
    return int(rows[0][0])


@pytest.fixture(scope="session")
def config() -> TestConfig:
    return _load_test_config()


@pytest.fixture(scope="module")
def executed_pipeline(config: TestConfig) -> PipelineRunResult:
    _run_command(["docker", "--version"], timeout_seconds=30)

    _wait_until(
        "MinIO readiness",
        lambda: bool(_build_minio_client(config).list_buckets() or True),
        timeout_seconds=180,
    )
    _wait_until(
        "PostgreSQL readiness",
        lambda: _can_connect_postgres(config),
        timeout_seconds=180,
    )
    _wait_until(
        "Airflow DAG availability",
        lambda: _airflow_cli_ready(config),
        timeout_seconds=300,
    )

    client = _build_minio_client(config)
    if not client.bucket_exists(config.minio_raw_bucket):
        client.make_bucket(config.minio_raw_bucket)
    if not client.bucket_exists(config.minio_processed_bucket):
        client.make_bucket(config.minio_processed_bucket)

    suffix = uuid.uuid4().hex[:12]
    object_name = f"e2e_{suffix}.csv"
    order_code = f"ORD-{suffix}"

    csv_payload = (
        "order_code,customer_code,customer_name,email,country,"
        "product_code,product_name,category,quantity,unit_price,order_date\n"
        f"{order_code},CUST-{suffix},E2E User,e2e.{suffix}@example.com,Ghana,"
        f"PROD-{suffix},E2E Product,Electronics,3,45.50,"
        f"{datetime.now(timezone.utc).date().isoformat()}\n"
    )
    csv_bytes = csv_payload.encode("utf-8")
    client.put_object(
        config.minio_raw_bucket,
        object_name,
        io.BytesIO(csv_bytes),
        length=len(csv_bytes),
        content_type="text/csv",
    )

    assert _object_exists(client, config.minio_raw_bucket, object_name), (
        f"Expected raw object {object_name} in bucket {config.minio_raw_bucket}."
    )

    _run_command(
        [
            "docker",
            "exec",
            config.airflow_container,
            "airflow",
            "dags",
            "unpause",
            config.dag_id,
        ],
        timeout_seconds=120,
    )
    _run_command(
        [
            "docker",
            "exec",
            config.airflow_container,
            "airflow",
            "dags",
            "test",
            config.dag_id,
            datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        ],
        timeout_seconds=900,
    )

    _wait_until(
        "object move from raw to processed bucket",
        lambda: (
            _object_exists(client, config.minio_processed_bucket, object_name)
            and not _object_exists(client, config.minio_raw_bucket, object_name)
        ),
        timeout_seconds=180,
    )

    _wait_until(
        "fact_sales load in PostgreSQL",
        lambda: _fact_sales_count_for_order(config, order_code) >= 1,
        timeout_seconds=180,
    )
    fact_sales_rows = _fact_sales_count_for_order(config, order_code)

    return PipelineRunResult(
        object_name=object_name,
        order_code=order_code,
        fact_sales_rows=fact_sales_rows,
    )


@pytest.mark.e2e
def test_pipeline_moves_data_from_minio_to_postgres(
    executed_pipeline: PipelineRunResult,
) -> None:
    assert executed_pipeline.fact_sales_rows >= 1


@pytest.mark.e2e
def test_metabase_can_query_loaded_fact_sales(
    config: TestConfig,
    executed_pipeline: PipelineRunResult,
) -> None:
    _wait_until(
        "Metabase API health",
        lambda: _metabase_health_ok(config),
        timeout_seconds=240,
        poll_interval_seconds=5,
    )

    session_id, database_id = _ensure_metabase_database(config)
    row_count = _query_order_count_via_metabase(
        config,
        session_id=session_id,
        database_id=database_id,
        order_code=executed_pipeline.order_code,
    )
    assert row_count >= 1
