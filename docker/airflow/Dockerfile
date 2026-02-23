FROM apache/airflow:2.8.1

USER airflow

RUN pip install --no-cache-dir \
    minio==7.2.5 \
    tenacity==8.2.3 \
    pandas==1.5.3 \
    psycopg2-binary==2.9.9