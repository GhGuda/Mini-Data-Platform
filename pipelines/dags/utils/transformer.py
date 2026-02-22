"""
Transformation Module
Converts raw sales CSV into normalized DataFrames.
"""

import logging
import pandas as pd


class SalesTransformer:
    """
    Handles transformation and normalization of raw sales data.
    """

    def __init__(self, logger: logging.Logger):
        self.logger = logger

    def load_raw(self, file_path: str) -> pd.DataFrame:
        """
        Load raw CSV file into DataFrame.
        """
        self.logger.info(f"Loading raw file: {file_path}")
        df = pd.read_csv(file_path)

        self.logger.info(f"Loaded {len(df)} records.")
        return df

    def transform(self, df: pd.DataFrame) -> dict:
        """
        Normalize raw DataFrame into separate DataFrames
        for customers, products, orders, order_items, and fact_sales.
        """

        self.logger.info("Starting transformation process.")

        # Drop duplicates for dimension tables
        customers = (
            df[["customer_code", "customer_name", "email", "country"]]
            .drop_duplicates()
            .reset_index(drop=True)
        )

        products = (
            df[["product_code", "product_name", "category"]]
            .drop_duplicates()
            .reset_index(drop=True)
        )

        orders = (
            df[["order_code", "customer_code", "order_date"]]
            .drop_duplicates()
            .reset_index(drop=True)
        )

        order_items = df[
            [
                "order_code",
                "product_code",
                "quantity",
                "unit_price",
            ]
        ].copy()

        order_items["total_price"] = (
            order_items["quantity"] * order_items["unit_price"]
        )

        fact_sales = order_items.copy()

        self.logger.info("Transformation completed successfully.")

        return {
            "customers": customers,
            "products": products,
            "orders": orders,
            "order_items": order_items,
            "fact_sales": fact_sales,
        }