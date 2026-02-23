"""
Database Utility Module
Handles PostgreSQL connection and insert logic.
"""

import os
import logging
from typing import Dict, Iterable
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

    def _execute_values(
        self,
        conn,
        query: str,
        values: list,
    ):
        """
        Execute execute_values for non-empty rows.
        """
        if not values:
            return

        with conn.cursor() as cursor:
            execute_values(cursor, query, values)

    def _fetch_id_map(
        self,
        conn,
        table_name: str,
        code_column: str,
        codes: Iterable[str],
    ) -> Dict[str, int]:
        """
        Fetch {business_code: id} for a subset of business keys.
        """
        unique_codes = list({code for code in codes if code})
        if not unique_codes:
            return {}

        with conn.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT {code_column}, id
                FROM {table_name}
                WHERE {code_column} = ANY(%s);
                """,
                (unique_codes,),
            )
            rows = cursor.fetchall()

        return {code: row_id for code, row_id in rows}

    def _table_columns(self, conn, table_name: str) -> set:
        """
        Return all column names for a table in public schema.
        """
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'public'
                  AND table_name = %s;
                """,
                (table_name,),
            )
            rows = cursor.fetchall()

        return {row[0] for row in rows}

    def _table_has_column(self, conn, table_name: str, column_name: str) -> bool:
        """
        Check whether a table has a given column in public schema.
        """
        return column_name in self._table_columns(conn, table_name)

    def _resolve_customer_name_column(self, conn) -> str:
        """
        Resolve active customer name column across schema versions.
        """
        has_full_name = self._table_has_column(conn, "customers", "full_name")
        has_customer_name = self._table_has_column(
            conn,
            "customers",
            "customer_name",
        )

        if has_full_name:
            return "full_name"

        if has_customer_name:
            self.logger.warning(
                "Using legacy customers.customer_name column; "
                "consider migrating schema to customers.full_name."
            )
            return "customer_name"

        raise ValueError(
            "customers table must have either 'full_name' "
            "or legacy 'customer_name' column."
        )

    def _load_legacy_schema(
        self,
        conn,
        customers_df,
        products_df,
        order_lines_df,
    ):
        """
        Backward-compatible load path for legacy denormalized schema.
        """
        name_column = self._resolve_customer_name_column(conn)

        customer_rows = [
            tuple(row)
            for row in customers_df[
                ["customer_code", "full_name", "email", "country"]
            ].itertuples(index=False, name=None)
        ]
        self._execute_values(
            conn,
            f"""
            INSERT INTO customers (customer_code, {name_column}, email, country)
            VALUES %s
            ON CONFLICT (customer_code)
            DO UPDATE SET
                {name_column} = EXCLUDED.{name_column},
                email = EXCLUDED.email,
                country = EXCLUDED.country;
            """,
            customer_rows,
        )
        self.logger.info("Upserted customers (legacy schema).")

        if self._table_has_column(conn, "products", "price"):
            product_rows = [
                tuple(row)
                for row in products_df[
                    ["product_code", "product_name", "category", "price"]
                ].itertuples(index=False, name=None)
            ]
            self._execute_values(
                conn,
                """
                INSERT INTO products (product_code, product_name, category, price)
                VALUES %s
                ON CONFLICT (product_code)
                DO UPDATE SET
                    product_name = EXCLUDED.product_name,
                    category = EXCLUDED.category,
                    price = EXCLUDED.price;
                """,
                product_rows,
            )
        else:
            product_rows = [
                tuple(row)
                for row in products_df[
                    ["product_code", "product_name", "category"]
                ].itertuples(index=False, name=None)
            ]
            self._execute_values(
                conn,
                """
                INSERT INTO products (product_code, product_name, category)
                VALUES %s
                ON CONFLICT (product_code)
                DO UPDATE SET
                    product_name = EXCLUDED.product_name,
                    category = EXCLUDED.category;
                """,
                product_rows,
            )
        self.logger.info("Upserted products (legacy schema).")

        orders_df = (
            order_lines_df[["order_code", "customer_code", "order_date"]]
            .drop_duplicates()
            .reset_index(drop=True)
        )
        order_rows = [
            tuple(row)
            for row in orders_df[
                ["order_code", "customer_code", "order_date"]
            ].itertuples(index=False, name=None)
        ]
        self._execute_values(
            conn,
            """
            INSERT INTO orders (order_code, customer_code, order_date)
            VALUES %s
            ON CONFLICT (order_code)
            DO UPDATE SET
                customer_code = EXCLUDED.customer_code,
                order_date = EXCLUDED.order_date;
            """,
            order_rows,
        )
        self.logger.info("Upserted orders (legacy schema).")

        metrics_df = order_lines_df[
            ["order_code", "product_code", "quantity", "unit_price", "line_total"]
        ].copy()
        metrics_df = metrics_df.rename(columns={"line_total": "total_price"})

        order_item_rows = [
            tuple(row)
            for row in metrics_df[
                ["order_code", "product_code", "quantity", "unit_price", "total_price"]
            ].itertuples(index=False, name=None)
        ]
        self._execute_values(
            conn,
            """
            INSERT INTO order_items (order_code, product_code, quantity, unit_price, total_price)
            VALUES %s
            ON CONFLICT (order_code, product_code)
            DO UPDATE SET
                quantity = EXCLUDED.quantity,
                unit_price = EXCLUDED.unit_price,
                total_price = EXCLUDED.total_price;
            """,
            order_item_rows,
        )
        self.logger.info("Upserted order_items (legacy schema).")

        self._execute_values(
            conn,
            """
            INSERT INTO fact_sales (order_code, product_code, quantity, unit_price, total_price)
            VALUES %s
            ON CONFLICT (order_code, product_code)
            DO UPDATE SET
                quantity = EXCLUDED.quantity,
                unit_price = EXCLUDED.unit_price,
                total_price = EXCLUDED.total_price;
            """,
            order_item_rows,
        )
        self.logger.info("Upserted fact_sales (legacy schema).")

    def load(self, normalized_data: Dict):
        """
        Insert all normalized datasets into DB inside a transaction.
        """

        conn = self.get_connection()

        try:
            conn.autocommit = False

            customers_df = normalized_data["customers"]
            products_df = normalized_data["products"]
            order_lines_df = normalized_data["order_lines"]

            orders_columns = self._table_columns(conn, "orders")
            order_items_columns = self._table_columns(conn, "order_items")
            fact_sales_columns = self._table_columns(conn, "fact_sales")

            is_legacy_schema = (
                "customer_code" in orders_columns
                and "customer_id" not in orders_columns
                and "order_code" in order_items_columns
                and "order_id" not in order_items_columns
                and "order_code" in fact_sales_columns
                and "order_id" not in fact_sales_columns
            )

            if is_legacy_schema:
                self.logger.warning(
                    "Detected legacy denormalized schema; "
                    "using compatibility load path."
                )
                self._load_legacy_schema(
                    conn,
                    customers_df,
                    products_df,
                    order_lines_df,
                )
                conn.commit()
                self.logger.info("Database transaction committed successfully.")
                return

            name_column = self._resolve_customer_name_column(conn)

            customer_rows = [
                tuple(row)
                for row in customers_df[
                    ["customer_code", "full_name", "email", "country"]
                ].itertuples(index=False, name=None)
            ]
            self._execute_values(
                conn,
                f"""
                INSERT INTO customers (customer_code, {name_column}, email, country)
                VALUES %s
                ON CONFLICT (customer_code)
                DO UPDATE SET
                    {name_column} = EXCLUDED.{name_column},
                    email = EXCLUDED.email,
                    country = EXCLUDED.country;
                """,
                customer_rows,
            )
            self.logger.info("Upserted customers.")

            product_rows = [
                tuple(row)
                for row in products_df[
                    ["product_code", "product_name", "category", "price"]
                ].itertuples(index=False, name=None)
            ]
            self._execute_values(
                conn,
                """
                INSERT INTO products (product_code, product_name, category, price)
                VALUES %s
                ON CONFLICT (product_code)
                DO UPDATE SET
                    product_name = EXCLUDED.product_name,
                    category = EXCLUDED.category,
                    price = EXCLUDED.price;
                """,
                product_rows,
            )
            self.logger.info("Upserted products.")

            customer_id_by_code = self._fetch_id_map(
                conn,
                "customers",
                "customer_code",
                order_lines_df["customer_code"].tolist(),
            )
            product_id_by_code = self._fetch_id_map(
                conn,
                "products",
                "product_code",
                order_lines_df["product_code"].tolist(),
            )

            orders_df = (
                order_lines_df.groupby(
                    ["order_code", "customer_code", "order_date"],
                    as_index=False,
                )["line_total"]
                .sum()
                .rename(columns={"line_total": "total_amount"})
            )
            orders_df["customer_id"] = orders_df["customer_code"].map(
                customer_id_by_code
            )
            if orders_df["customer_id"].isnull().any():
                missing_customers = sorted(
                    orders_df[orders_df["customer_id"].isnull()][
                        "customer_code"
                    ].unique()
                )
                raise ValueError(
                    "Missing customer IDs for order load: "
                    + ", ".join(missing_customers)
                )
            orders_df["customer_id"] = orders_df["customer_id"].astype(int)

            order_rows = [
                tuple(row)
                for row in orders_df[
                    ["order_code", "customer_id", "order_date", "total_amount"]
                ].itertuples(index=False, name=None)
            ]
            self._execute_values(
                conn,
                """
                INSERT INTO orders (order_code, customer_id, order_date, total_amount)
                VALUES %s
                ON CONFLICT (order_code)
                DO UPDATE SET
                    customer_id = EXCLUDED.customer_id,
                    order_date = EXCLUDED.order_date,
                    total_amount = EXCLUDED.total_amount;
                """,
                order_rows,
            )
            self.logger.info("Upserted orders.")

            order_id_by_code = self._fetch_id_map(
                conn,
                "orders",
                "order_code",
                order_lines_df["order_code"].tolist(),
            )

            line_items = order_lines_df.copy()
            line_items["order_id"] = line_items["order_code"].map(order_id_by_code)
            line_items["product_id"] = line_items["product_code"].map(
                product_id_by_code
            )
            line_items["customer_id"] = line_items["customer_code"].map(
                customer_id_by_code
            )

            if line_items["order_id"].isnull().any():
                missing_orders = sorted(
                    line_items[line_items["order_id"].isnull()][
                        "order_code"
                    ].unique()
                )
                raise ValueError(
                    "Missing order IDs for order_items load: "
                    + ", ".join(missing_orders)
                )
            if line_items["product_id"].isnull().any():
                missing_products = sorted(
                    line_items[line_items["product_id"].isnull()][
                        "product_code"
                    ].unique()
                )
                raise ValueError(
                    "Missing product IDs for order_items load: "
                    + ", ".join(missing_products)
                )
            if line_items["customer_id"].isnull().any():
                missing_customers = sorted(
                    line_items[line_items["customer_id"].isnull()][
                        "customer_code"
                    ].unique()
                )
                raise ValueError(
                    "Missing customer IDs for fact_sales load: "
                    + ", ".join(missing_customers)
                )
            line_items["order_id"] = line_items["order_id"].astype(int)
            line_items["product_id"] = line_items["product_id"].astype(int)
            line_items["customer_id"] = line_items["customer_id"].astype(int)

            order_item_rows = [
                tuple(row)
                for row in line_items[
                    ["order_id", "product_id", "quantity", "unit_price", "line_total"]
                ].itertuples(index=False, name=None)
            ]
            self._execute_values(
                conn,
                """
                INSERT INTO order_items (order_id, product_id, quantity, unit_price, line_total)
                VALUES %s;
                """,
                order_item_rows,
            )
            self.logger.info("Inserted order_items.")

            fact_sales_rows = [
                tuple(row)
                for row in line_items[
                    [
                        "order_id",
                        "order_date",
                        "customer_id",
                        "country",
                        "product_id",
                        "category",
                        "quantity",
                        "line_total",
                    ]
                ].itertuples(index=False, name=None)
            ]
            self._execute_values(
                conn,
                """
                INSERT INTO fact_sales (
                    order_id,
                    order_date,
                    customer_id,
                    customer_country,
                    product_id,
                    product_category,
                    quantity,
                    revenue
                )
                VALUES %s;
                """,
                fact_sales_rows,
            )
            self.logger.info("Inserted fact_sales.")

            conn.commit()
            self.logger.info("Database transaction committed successfully.")

        except Exception:
            conn.rollback()
            self.logger.exception("Transaction failed. Rolled back.")
            raise
        finally:
            conn.close()
