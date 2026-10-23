"""Ingestion step — local mirror of S3 partitioned landing.

Writes raw orders as CSV files partitioned by region, using the Hive-style
``region=<value>/`` layout an S3 landing bucket (and Athena / Glue crawlers)
would use:

    data/bronze/region=Northeast/orders.csv
    data/bronze/region=Southeast/orders.csv
    ...
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)


def ingest(df: pd.DataFrame, bronze_dir: Path) -> dict[str, int]:
    """Write *df* to partitioned raw CSVs under *bronze_dir*.

    Returns a mapping of partition name -> row count written.
    """
    bronze_dir = Path(bronze_dir)
    bronze_dir.mkdir(parents=True, exist_ok=True)
    counts: dict[str, int] = {}

    # Rows with a missing region still land (validation rejects them later);
    # file them under a literal partition so nothing is silently dropped.
    work = df.copy()
    work["region"] = work["region"].fillna("Unknown")

    for region, group in work.groupby("region"):
        partition_dir = bronze_dir / f"region={region}"
        partition_dir.mkdir(parents=True, exist_ok=True)
        out = partition_dir / "orders.csv"
        group.to_csv(out, index=False)
        counts[str(region)] = len(group)
        logger.info("Bronze partition region=%s: %d rows -> %s", region, len(group), out)

    logger.info("Ingestion complete: %d rows across %d partitions", sum(counts.values()), len(counts))
    return counts


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    from generate_data import generate_orders

    ingest(generate_orders(), Path("data/bronze"))
