# Architecture and Data Flow

## High-Level Data Flow
```mermaid
flowchart LR
    S[Sales CSV Files] --> MR[MinIO: sales-raw]
    MR --> A[Airflow Scheduler and DAG Execution]
    A --> T[ETL Transform and Validation]
    T --> P[(PostgreSQL: sales_db)]
    A --> MP[MinIO: sales-processed]
    P --> M[Metabase Dashboards]
```

## Runtime Component View
```mermaid
flowchart TB
    subgraph Ingestion
        DG[Data Generator Service]
        ST[Storage Init Service]
        MINIO[MinIO]
    end

    subgraph Orchestration
        AWS[Airflow Webserver]
        AS[Airflow Scheduler]
        DAG[sales_etl_pipeline DAG]
    end

    subgraph Storage
        PG[(PostgreSQL)]
        FS[(fact_sales table)]
    end

    subgraph Analytics
        MB[Metabase]
    end

    DG --> MINIO
    ST --> MINIO
    MINIO --> DAG
    AS --> DAG
    AWS --> DAG
    DAG --> PG
    PG --> FS
    MB --> PG
```

## Data Flow Stages
1. Raw data enters MinIO raw bucket (`sales-raw`).
2. Airflow DAG (`sales_etl_pipeline`) discovers and downloads pending CSV files.
3. ETL logic validates and transforms records into target model.
4. Transformed records are loaded into PostgreSQL analytical tables.
5. Successfully processed files are moved to MinIO processed bucket (`sales-processed`).
6. Metabase reads PostgreSQL for reporting and dashboards.

## CI/CD Flow
```mermaid
flowchart LR
    G[Git Push] --> CI[GitHub Actions CI]
    CI --> T[Test ETL Pipeline]
    T --> I[Build and Tag Images]
    I --> R[Push to GHCR]
    R --> CD[Self-Hosted Deploy Job]
    CD --> PROD[Production Docker Compose]
```
