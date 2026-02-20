#!/bin/bash

set -e

echo "Waiting for PostgreSQL..."

until pg_isready -h ${POSTGRES_HOST} -p ${POSTGRES_PORT} -U ${POSTGRES_USER}; do
  sleep 2
done

echo "PostgreSQL is ready."

echo "Initializing Airflow DB..."
airflow db upgrade

echo "Creating admin user if not exists..."

airflow users create \
  --username admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com \
  --password admin || true

echo "Starting Airflow $1..."
exec airflow "$@"