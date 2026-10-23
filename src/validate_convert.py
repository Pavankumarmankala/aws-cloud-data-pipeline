"""Validation + Parquet conversion — local mirror of the Lambda step.

In AWS, a Lambda function triggered on S3 upload validates each file and
converts it to Parquet. Locally this module:

1. reads every partitioned CSV under ``data/bronze/``,
2. validates each row (required fields, positive quantity / prices /
   totals, known region, parseable date),
3. writes valid rows to ``data/silver/orders.parquet`` (partitioned by
   region on disk, via pyarrow),
4. writes rejected rows to ``data/silver/rejected_rows.csv`` for audit,
5. logs a validation summary (the local stand-in for CloudWatch metrics).
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from .generate_data import REGIONS

logger = logging.getLogger(__name__)

REQUIRED_COLUMNS = [
    "order_id",
    "order_date",
    "customer_id",
    "product_id",
    "product_name",
    "category",
    "region",
    "quantity",
    "unit_price",
    "discount_pct",
    "total_amount",
    "payment_method",
    "order_status",
]


def _read_bronze(bronze_dir: Path) -> pd.DataFrame:
    files = sorted(Path(bronze_dir).rglob("orders.csv"))
    if not files:
        raise FileNotFoundError(f"No bronze CSV files found under {bronze_dir}")
    frames = [pd.read_csv(f) for f in files]
    df = pd.concat(frames, ignore_index=True)
    logger.info("Read %d rows from %d bronze files", len(df), len(files))
    return df


def _validate(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"Bronze data is missing required columns: {missing}")

    work = df.copy()
    work["order_date"] = pd.to_datetime(work["order_date"], errors="coerce")

    valid_mask = (
        work["order_id"].notna()
        & work["customer_id"].notna()
        & work["product_id"].notna()
        & work["category"].notna()
        & work["order_date"].notna()
        & work["region"].isin(REGIONS)
        & (work["quantity"] > 0)
        & (work["unit_price"] > 0)
        & (work["total_amount"] > 0)
        & work["discount_pct"].between(0, 1)
    )
    valid = work.loc[valid_mask].copy()
    rejected = work.loc[~valid_mask].copy()

    valid["order_date"] = valid["order_date"].dt.date.astype(str)
    valid["quantity"] = valid["quantity"].astype(int)
    return valid, rejected


def validate_and_convert(bronze_dir: Path, silver_dir: Path) -> dict:
    """Validate bronze CSVs and write silver Parquet. Returns a summary dict."""
    silver_dir = Path(silver_dir)
    silver_dir.mkdir(parents=True, exist_ok=True)

    raw = _read_bronze(Path(bronze_dir))
    valid, rejected = _validate(raw)

    parquet_path = silver_dir / "orders.parquet"
    # Partitioned Parquet dataset (region=.../), as Lambda + S3 would lay out.
    valid.to_parquet(parquet_path, index=False, partition_cols=["region"])
    rejected_path = silver_dir / "rejected_rows.csv"
    rejected.to_csv(rejected_path, index=False)

    summary = {
        "rows_in": int(len(raw)),
        "rows_valid": int(len(valid)),
        "rows_rejected": int(len(rejected)),
        "rejection_rate_pct": round(100 * len(rejected) / max(len(raw), 1), 2),
        "total_revenue": round(float(valid["total_amount"].sum()), 2),
        "silver_path": str(parquet_path),
        "rejected_path": str(rejected_path),
    }
    logger.info(
        "Validation summary: in=%d valid=%d rejected=%d (%.2f%%) revenue=$%.2f",
        summary["rows_in"],
        summary["rows_valid"],
        summary["rows_rejected"],
        summary["rejection_rate_pct"],
        summary["total_revenue"],
    )
    return summary
