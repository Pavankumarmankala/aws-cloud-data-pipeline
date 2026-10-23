"""Aggregation step — local mirror of Glue ETL.

Reads the silver Parquet dataset and produces exactly 6 gold analytics
tables (one Parquet file each) under ``data/gold/``:

    1. daily_revenue.parquet
    2. revenue_by_category.parquet
    3. revenue_by_region.parquet
    4. top_products.parquet
    5. customer_summary.parquet
    6. monthly_summary.parquet

In AWS these are the tables a Glue job writes back to S3 for Athena
querying and Redshift loading.
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)


def _daily_revenue(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df.groupby("order_date", as_index=False)
        .agg(orders=("order_id", "count"), revenue=("total_amount", "sum"),
             units_sold=("quantity", "sum"), avg_order_value=("total_amount", "mean"))
        .sort_values("order_date")
    )


def _revenue_by_category(df: pd.DataFrame) -> pd.DataFrame:
    out = (
        df.groupby("category", as_index=False)
        .agg(orders=("order_id", "count"), revenue=("total_amount", "sum"),
             units_sold=("quantity", "sum"), avg_order_value=("total_amount", "mean"))
        .sort_values("revenue", ascending=False)
    )
    out["revenue_share_pct"] = (100 * out["revenue"] / out["revenue"].sum()).round(2)
    return out


def _revenue_by_region(df: pd.DataFrame) -> pd.DataFrame:
    out = (
        df.groupby("region", as_index=False)
        .agg(orders=("order_id", "count"), revenue=("total_amount", "sum"),
             units_sold=("quantity", "sum"), unique_customers=("customer_id", "nunique"),
             avg_order_value=("total_amount", "mean"))
        .sort_values("revenue", ascending=False)
    )
    out["revenue_share_pct"] = (100 * out["revenue"] / out["revenue"].sum()).round(2)
    return out


def _top_products(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df.groupby(["product_id", "product_name", "category"], as_index=False)
        .agg(orders=("order_id", "count"), revenue=("total_amount", "sum"),
             units_sold=("quantity", "sum"))
        .sort_values("revenue", ascending=False)
        .head(20)
        .reset_index(drop=True)
    )


def _customer_summary(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df.groupby("customer_id", as_index=False)
        .agg(orders=("order_id", "count"), total_spend=("total_amount", "sum"),
             units_bought=("quantity", "sum"), first_order=("order_date", "min"),
             last_order=("order_date", "max"))
        .sort_values("total_spend", ascending=False)
    )


def _monthly_summary(df: pd.DataFrame) -> pd.DataFrame:
    work = df.copy()
    work["year_month"] = work["order_date"].str.slice(0, 7)
    return (
        work.groupby("year_month", as_index=False)
        .agg(orders=("order_id", "count"), revenue=("total_amount", "sum"),
             units_sold=("quantity", "sum"), unique_customers=("customer_id", "nunique"),
             avg_order_value=("total_amount", "mean"))
        .sort_values("year_month")
    )


BUILDERS = {
    "daily_revenue": _daily_revenue,
    "revenue_by_category": _revenue_by_category,
    "revenue_by_region": _revenue_by_region,
    "top_products": _top_products,
    "customer_summary": _customer_summary,
    "monthly_summary": _monthly_summary,
}


def aggregate(silver_dir: Path, gold_dir: Path) -> dict[str, int]:
    """Build the 6 gold tables from silver Parquet. Returns table -> row count."""
    gold_dir = Path(gold_dir)
    gold_dir.mkdir(parents=True, exist_ok=True)

    df = pd.read_parquet(Path(silver_dir) / "orders.parquet")
    logger.info("Loaded %d silver rows for aggregation", len(df))

    counts: dict[str, int] = {}
    for name, builder in BUILDERS.items():
        table = builder(df)
        out = gold_dir / f"{name}.parquet"
        table.to_parquet(out, index=False)
        counts[name] = len(table)
        logger.info("Gold table %-22s %5d rows -> %s", name, len(table), out)

    assert len(counts) == 6, f"Expected exactly 6 gold tables, built {len(counts)}"
    return counts
