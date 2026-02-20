CSV Upload
     ↓
MinIO (Raw Storage)
     ↓
Airflow (Process + Transform)
     ↓
PostgreSQL (Structured Storage)
     ↓
Metabase (Visualization)



🧭 High-Level Implementation Roadmap

- We’ll break the project into 8 controlled stages:

- Project Foundation & Repository Structure

- Docker Compose Infrastructure (Networking + Services)

- PostgreSQL Initialization & Schema Design

- MinIO Configuration & Ingestion Strategy

- Airflow Setup + Production-Ready DAG Design

- Data Generator (Robust, Scalable, Observable)

- Metabase Integration & Analytics Layer

- CI/CD + Data Flow Validation Automation