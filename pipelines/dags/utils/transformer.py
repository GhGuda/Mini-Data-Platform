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
        Normalize raw DataFrame into dimension tables and order line items.
        """

        self.logger.info("Starting transformation process.")

        required_columns = {
            "order_code",
            "customer_code",
            "customer_name",
            "email",
            "country",
            "product_code",
            "product_name",
            "category",
            "quantity",
            "unit_price",
            "order_date",
        }

        missing_columns = sorted(required_columns.difference(df.columns))
        if missing_columns:
            raise ValueError(
                f"Missing required column(s): {', '.join(missing_columns)}"
            )

        working_df = df.copy()
        working_df["quantity"] = pd.to_numeric(
            working_df["quantity"],
            errors="raise",
        )
        working_df["unit_price"] = pd.to_numeric(
            working_df["unit_price"],
            errors="raise",
        )
        working_df["order_date"] = pd.to_datetime(
            working_df["order_date"],
            errors="raise",
        ).dt.date

        customers = (
            working_df[["customer_code", "customer_name", "email", "country"]]
            .rename(columns={"customer_name": "full_name"})
            .drop_duplicates()
            .reset_index(drop=True)
        )

        products = (
            working_df[["product_code", "product_name", "category", "unit_price"]]
            .rename(columns={"unit_price": "price"})
            .drop_duplicates(subset=["product_code"], keep="last")
            .reset_index(drop=True)
        )

        order_lines = (
            working_df[
                [
                    "order_code",
                    "customer_code",
                    "order_date",
                    "country",
                    "product_code",
                    "category",
                    "quantity",
                    "unit_price",
                ]
            ]
            .reset_index(drop=True)
        )

        order_lines["line_total"] = (
            order_lines["quantity"] * order_lines["unit_price"]
        )

        self.logger.info("Transformation completed successfully.")

        return {
            "customers": customers,
            "products": products,
            "order_lines": order_lines,
        }
