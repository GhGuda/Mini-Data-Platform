"""
Database Utility Module
Handles PostgreSQL connection and insert logic.
"""

import os
import logging
from typing import Dict
import psycopg2
from psycopg2.extras import execute_values
from tenacity import retry, stop_after_attempt, wait_exponential


class PostgresLoader:
    """
    Handles inserting normalized data into PostgreSQL.
    """

    def __init__(self, logger: logging.Logger):
        self.logger = logger

        self.conn_params = {
            "host": os.getenv("POSTGRES_HOST", "postgres"),
            "port": os.getenv("POSTGRES_PORT", 5432),
            "dbname": os.getenv("POSTGRES_DB"),
            "user": os.getenv("POSTGRES_USER"),
            "password": os.getenv("POSTGRES_PASSWORD"),
        }

    @retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=2))
    def get_connection(self):
        """
        Establish database connection with retry.
        """
        self.logger.info("Connecting to PostgreSQL...")
        return psycopg2.connect(**self.conn_params)

    def upsert_dataframe(
        self,
        conn,
        df,
        table_name: str,
        conflict_column: str,
    ):
        """
        Bulk insert with ON CONFLICT DO NOTHING.
        """

        if df.empty:
            self.logger.info(f"No records to insert into {table_name}.")
            return

        columns = list(df.columns)
        values = [tuple(row) for row in df.to_numpy()]

        insert_query = f"""
            INSERT INTO {table_name} ({','.join(columns)})
            VALUES %s
            ON CONFLICT ({conflict_column}) DO NOTHING;
        """

        with conn.cursor() as cur:
            execute_values(cur, insert_query, values)

        self.logger.info(f"Inserted records into {table_name}.")

    def load(self, normalized_data: Dict):
        """
        Insert all normalized datasets into DB inside a transaction.
        """

        conn = self.get_connection()

        try:
            conn.autocommit = False

            self.upsert_dataframe(
                conn,
                normalized_data["customers"],
                "customers",
                "customer_code",
            )

            self.upsert_dataframe(
                conn,
                normalized_data["products"],
                "products",
                "product_code",
            )

            self.upsert_dataframe(
                conn,
                normalized_data["orders"],
                "orders",
                "order_code",
            )

            self.upsert_dataframe(
                conn,
                normalized_data["order_items"],
                "order_items",
                "order_code, product_code",
            )

            self.upsert_dataframe(
                conn,
                normalized_data["fact_sales"],
                "fact_sales",
                "order_code, product_code",
            )

            conn.commit()
            self.logger.info("Database transaction committed successfully.")

        except Exception:
            conn.rollback()
            self.logger.exception("Transaction failed. Rolled back.")
            raise
        finally:
            conn.close()