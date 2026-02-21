import os
import random
import logging
from datetime import datetime, timedelta
from typing import List, Dict

import pandas as pd
from faker import Faker

fake = Faker()


class SalesDataGenerator:
    """
    Generates synthetic sales data aligned with the sales schema.
    """

    def __init__(self, record_count: int, logger: logging.Logger):
        """
        Initialize generator with record count and logger.
        """
        self.record_count = record_count
        self.logger = logger

    def _generate_single_record(self) -> Dict:
        """
        Generate a single synthetic sales record.
        """
        quantity = random.randint(1, 5)
        unit_price = round(random.uniform(10, 500), 2)

        return {
            "order_code": f"ORD-{fake.uuid4()}",
            "customer_code": f"CUST-{fake.uuid4()}",
            "customer_name": fake.name(),
            "email": fake.email(),
            "country": fake.country(),
            "product_code": f"PROD-{fake.uuid4()}",
            "product_name": fake.word().capitalize(),
            "category": random.choice(["Electronics", "Clothing", "Books", "Home", "Sports"]),
            "quantity": quantity,
            "unit_price": unit_price,
            "order_date": (
                datetime.utcnow() - timedelta(days=random.randint(0, 30))
            ).strftime("%Y-%m-%d"),
        }

    def generate(self) -> pd.DataFrame:
        """
        Generate a DataFrame of synthetic sales records.
        """
        self.logger.info(f"Generating {self.record_count} sales records.")

        data = [self._generate_single_record() for _ in range(self.record_count)]

        df = pd.DataFrame(data)

        self.logger.info("Sales data generation completed.")
        return df