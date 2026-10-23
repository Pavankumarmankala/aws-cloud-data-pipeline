# AWS Cloud Data Pipeline

End-to-end cloud data pipeline processing **8,000+ e-commerce orders
($3.2M revenue)** — S3 partitioned ingestion, Lambda validation and
Parquet conversion, Glue ETL aggregation into **6 analytics tables**,
Athena serverless querying (**12 SQL queries**), and Redshift star
schema loading. Built with a Medallion architecture (raw CSV →
processed Parquet → analytics aggregates), CloudWatch monitoring, and
IAM role-based access control for pipeline governance.

This repository is a runnable local implementation of that pipeline.
Each local module mirrors one AWS step, so the flow can be run and
inspected without an AWS account.

## Architecture — Medallion

```
Bronze (raw)        Silver (processed)     Gold (analytics)
data/bronze/   ->   data/silver/      ->   data/gold/
partitioned CSV     validated Parquet      6 aggregate tables
region=.../         region-partitioned     (Parquet)
```

| Local module | AWS service it mirrors | What it does |
|---|---|---|
| `src/generate_data.py` | Source e-commerce system | Seeded synthetic orders: 8,865 valid + 75 deliberately corrupted rows |
| `src/ingest.py` | S3 partitioned landing | Writes raw CSVs to `data/bronze/region=<region>/orders.csv` |
| `src/validate_convert.py` | Lambda | Validates rows, rejects invalid ones to `rejected_rows.csv`, converts valid rows to Parquet in `data/silver/` |
| `src/aggregate.py` | Glue ETL | Builds exactly 6 gold analytics tables in `data/gold/` |
| `sql/athena_queries.sql` | Athena | 12 numbered serverless SQL queries over silver/gold |
| `sql/redshift_schema.sql` | Redshift | Star schema DDL: `fact_orders` + `dim_date`, `dim_customer`, `dim_product`, `dim_region` |
| Python `logging` | CloudWatch | Structured logs + validation summary at every step (local stand-in for CloudWatch Logs/metrics) |
| — | IAM | AWS-side configuration only: bucket policies and Lambda/Glue/Redshift IAM roles are provisioned in AWS; there is no local equivalent in code (see Notes) |

### Gold tables (exactly 6)

1. `daily_revenue` — orders, revenue, units, AOV by day
2. `revenue_by_category` — revenue and share by product category
3. `revenue_by_region` — revenue, customers, and share by region
4. `top_products` — top 20 products by revenue
5. `customer_summary` — per-customer orders, spend, first/last order
6. `monthly_summary` — monthly revenue, customers, and AOV

## Tech stack

- Python 3, pandas, pyarrow (Parquet)
- AWS (design): S3, Lambda, Glue, Athena, Redshift, CloudWatch, IAM
- SQL: Athena (Presto/Trino dialect) and Redshift DDL

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Run

```bash
python run_pipeline.py
```

This generates the synthetic orders, lands them in bronze, validates
and converts them to silver Parquet, and builds the 6 gold tables.
It prints a summary: rows in / valid / rejected, total revenue, and
the row count of each gold table.

Outputs (git-ignored, regenerated on each run):

- `data/bronze/region=<region>/orders.csv`
- `data/silver/orders.parquet/` (partitioned dataset) and `data/silver/rejected_rows.csv`
- `data/gold/<table>.parquet` — 6 files

## Key results

As described for the AWS pipeline:

- **8,000+ e-commerce orders** processed
- **$3.2M revenue** processed
- **6 analytics tables** produced by the aggregation (Glue) step
- **12 SQL queries** in `sql/athena_queries.sql`
- Medallion flow: raw CSV → processed Parquet → analytics aggregates

The local synthetic run is seeded (seed 42) and deterministic, and is
tuned to the same scale: 8,865 valid orders with total revenue
landing near $3.2M. Run `python run_pipeline.py` to see the exact
figures for the local run in its summary output.

## Project structure

```
aws-cloud-data-pipeline/
├── README.md
├── requirements.txt
├── run_pipeline.py          # end-to-end entry point
├── src/
│   ├── generate_data.py     # synthetic orders (source system)
│   ├── ingest.py            # bronze: partitioned CSV  (~ S3)
│   ├── validate_convert.py  # silver: validate + Parquet (~ Lambda)
│   └── aggregate.py         # gold: 6 tables           (~ Glue)
├── sql/
│   ├── athena_queries.sql   # 12 Athena queries
│   └── redshift_schema.sql  # Redshift star schema DDL
└── data/                    # generated at run time (git-ignored)
    ├── bronze/  silver/  gold/
```

## Notes

- **IAM** is AWS-side configuration (roles/policies for Lambda, Glue,
  Athena, and Redshift, plus S3 bucket policies). It governs the AWS
  deployment and has no local code equivalent; it is documented here
  rather than implemented.
- **CloudWatch** is represented locally by Python `logging` — every
  step logs progress and the validation step logs a summary that
  mirrors the metrics CloudWatch would collect.
- No AWS account or credentials are needed to run this repository.
