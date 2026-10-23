"""Run the full AWS Cloud Data Pipeline locally, end to end.

    python run_pipeline.py

Steps (local module -> AWS service it mirrors):
    1. Generate synthetic orders          -> source e-commerce system
    2. Ingest to data/bronze/ (partitioned CSV)   -> S3 landing bucket
    3. Validate + convert to Parquet in data/silver/ -> Lambda
    4. Aggregate to 6 gold tables in data/gold/   -> Glue ETL
    5. Query / warehouse layer            -> Athena (sql/athena_queries.sql)
                                              Redshift (sql/redshift_schema.sql)

Python ``logging`` is used throughout as the local stand-in for
CloudWatch Logs / metrics.
"""

from __future__ import annotations

import logging
from pathlib import Path

from src.aggregate import aggregate
from src.generate_data import generate_orders
from src.ingest import ingest
from src.validate_convert import validate_and_convert

BASE_DIR = Path(__file__).resolve().parent
BRONZE_DIR = BASE_DIR / "data" / "bronze"
SILVER_DIR = BASE_DIR / "data" / "silver"
GOLD_DIR = BASE_DIR / "data" / "gold"


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    log = logging.getLogger("run_pipeline")
    log.info("Pipeline started")

    orders = generate_orders()
    ingest(orders, BRONZE_DIR)
    summary = validate_and_convert(BRONZE_DIR, SILVER_DIR)
    gold_counts = aggregate(SILVER_DIR, GOLD_DIR)

    log.info("Pipeline finished successfully")
    print("\n=== Pipeline summary ===")
    print(f"Bronze rows in       : {summary['rows_in']}")
    print(f"Silver rows valid    : {summary['rows_valid']}")
    print(f"Rows rejected        : {summary['rows_rejected']} ({summary['rejection_rate_pct']}%)")
    print(f"Total revenue (valid): ${summary['total_revenue']:,.2f}")
    print(f"Gold tables built    : {len(gold_counts)}")
    for name, rows in gold_counts.items():
        print(f"  - {name:<24} {rows} rows")
    print(f"\nOutputs: {BRONZE_DIR} | {SILVER_DIR} | {GOLD_DIR}")


if __name__ == "__main__":
    main()
