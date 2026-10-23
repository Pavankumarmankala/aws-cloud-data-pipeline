"""Synthetic e-commerce order generator.

Mirrors the source system that feeds the AWS pipeline. Produces a
deterministic (seeded) dataset of valid orders plus a small number of
deliberately corrupted rows, so the validation step downstream has
realistic bad data to reject — as a production feed would.

Scale (matching the project description):
    8,865 valid orders, ~$3.2M total revenue, 75 corrupted rows.
"""

from __future__ import annotations

import logging
import random
from datetime import date, timedelta

import pandas as pd

logger = logging.getLogger(__name__)

SEED = 42
NUM_VALID_ORDERS = 8_865
NUM_INVALID_ROWS = 75
NUM_CUSTOMERS = 2_000

# product_id, product_name, category, base_price (USD)
PRODUCTS = [
    ("P001", "Wireless Noise-Cancelling Headphones", "Electronics", 749.00),
    ("P002", "4K Smart TV 55-inch", "Electronics", 549.00),
    ("P003", "Bluetooth Speaker", "Electronics", 399.00),
    ("P004", "USB-C Docking Station", "Electronics", 279.00),
    ("P005", "Stand Mixer", "Home & Kitchen", 349.00),
    ("P006", "Air Fryer XL", "Home & Kitchen", 249.00),
    ("P007", "Ceramic Dinnerware Set", "Home & Kitchen", 179.00),
    ("P008", "Winter Jacket", "Clothing", 179.00),
    ("P009", "Running Shoes", "Clothing", 119.00),
    ("P010", "Cotton T-Shirt 3-Pack", "Clothing", 89.00),
    ("P011", "Data Science Handbook", "Books", 69.00),
    ("P012", "Classic Novel Box Set", "Books", 39.00),
    ("P013", "Illustrated Cookbook", "Books", 49.00),
    ("P014", "Mountain Bike", "Sports", 279.00),
    ("P015", "Yoga Mat Pro", "Sports", 199.00),
    ("P016", "Dumbbell Set 20lb", "Sports", 139.00),
    ("P017", "Vitamin C Serum", "Beauty", 109.00),
    ("P018", "Moisturiser SPF 30", "Beauty", 79.00),
    ("P019", "Lipstick Set", "Beauty", 59.00),
    ("P020", "Perfume 50ml", "Beauty", 119.00),
]

REGIONS = ["Northeast", "Southeast", "Midwest", "Southwest", "West"]
PAYMENT_METHODS = ["credit_card", "debit_card", "paypal", "apple_pay"]
ORDER_STATUSES = ["completed", "completed", "completed", "completed", "shipped"]


def _valid_orders(rng: random.Random, num_orders: int) -> list[dict]:
    start = date(2024, 1, 1)
    rows: list[dict] = []
    for i in range(1, num_orders + 1):
        product_id, product_name, category, base_price = rng.choice(PRODUCTS)
        unit_price = round(base_price * rng.uniform(0.90, 1.10), 2)
        quantity = rng.choices([1, 2, 3, 4], weights=[0.50, 0.30, 0.15, 0.05])[0]
        discount_pct = rng.choices(
            [0.0, 0.05, 0.10, 0.15], weights=[0.70, 0.15, 0.10, 0.05]
        )[0]
        total_amount = round(quantity * unit_price * (1 - discount_pct), 2)
        rows.append(
            {
                "order_id": f"ORD-{i:05d}",
                "order_date": (start + timedelta(days=rng.randint(0, 365))).isoformat(),
                "customer_id": f"CUST-{rng.randint(1, NUM_CUSTOMERS):04d}",
                "product_id": product_id,
                "product_name": product_name,
                "category": category,
                "region": rng.choice(REGIONS),
                "quantity": quantity,
                "unit_price": unit_price,
                "discount_pct": discount_pct,
                "total_amount": total_amount,
                "payment_method": rng.choice(PAYMENT_METHODS),
                "order_status": rng.choice(ORDER_STATUSES),
            }
        )
    return rows


def _corrupted_rows(rng: random.Random, valid: list[dict], count: int) -> list[dict]:
    """Copy valid rows and corrupt one field each, cycling fault types."""
    rows: list[dict] = []
    for i in range(count):
        row = dict(rng.choice(valid))
        row["order_id"] = f"BAD-{i + 1:05d}"
        fault = i % 5
        if fault == 0:
            row["customer_id"] = None
        elif fault == 1:
            row["quantity"] = 0
        elif fault == 2:
            row["unit_price"] = None
        elif fault == 3:
            row["total_amount"] = -abs(row["total_amount"])
        else:
            row["region"] = "Unknown"
        rows.append(row)
    return rows


def generate_orders(
    num_valid: int = NUM_VALID_ORDERS,
    num_invalid: int = NUM_INVALID_ROWS,
    seed: int = SEED,
) -> pd.DataFrame:
    """Return a shuffled DataFrame of valid + deliberately corrupted orders."""
    rng = random.Random(seed)
    valid = _valid_orders(rng, num_valid)
    corrupted = _corrupted_rows(rng, valid, num_invalid)
    df = pd.DataFrame(valid + corrupted).sample(frac=1.0, random_state=seed)
    df = df.reset_index(drop=True)

    valid_revenue = df.loc[df["order_id"].str.startswith("ORD-"), "total_amount"].sum()
    logger.info(
        "Generated %d rows (%d valid, %d corrupted); valid revenue = $%.2f",
        len(df),
        num_valid,
        num_invalid,
        valid_revenue,
    )
    return df


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    frame = generate_orders()
    print(frame.head())
    print(f"rows={len(frame)}")
