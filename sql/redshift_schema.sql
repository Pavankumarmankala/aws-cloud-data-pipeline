-- Redshift star schema for the AWS Cloud Data Pipeline
-- Gold aggregates and silver orders are loaded from S3 into this schema
-- for warehouse-style analytics. Fact table: fact_orders.
-- Dimensions: dim_date, dim_customer, dim_product, dim_region.

CREATE SCHEMA IF NOT EXISTS analytics;

-- ---------------------------------------------------------------------------
-- Dimension: date
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics.dim_date (
    date_key        DATE        NOT NULL PRIMARY KEY,
    year            INT         NOT NULL,
    quarter         INT         NOT NULL,
    month           INT         NOT NULL,
    month_name      VARCHAR(20) NOT NULL,
    day             INT         NOT NULL,
    day_of_week     VARCHAR(20) NOT NULL,
    is_weekend      BOOLEAN     NOT NULL
)
DISTSTYLE ALL
SORTKEY (date_key);

-- ---------------------------------------------------------------------------
-- Dimension: customer (one row per customer, from customer_summary)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics.dim_customer (
    customer_key    VARCHAR(20) NOT NULL PRIMARY KEY,
    customer_id     VARCHAR(20) NOT NULL,
    total_orders    INT,
    total_spend     DECIMAL(12, 2),
    first_order     DATE,
    last_order      DATE
)
DISTSTYLE ALL
SORTKEY (customer_key);

-- ---------------------------------------------------------------------------
-- Dimension: product
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics.dim_product (
    product_key     VARCHAR(20)   NOT NULL PRIMARY KEY,
    product_id      VARCHAR(20)   NOT NULL,
    product_name    VARCHAR(200)  NOT NULL,
    category        VARCHAR(100)  NOT NULL,
    list_price      DECIMAL(10, 2)
)
DISTSTYLE ALL
SORTKEY (product_key);

-- ---------------------------------------------------------------------------
-- Dimension: region
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics.dim_region (
    region_key      VARCHAR(50) NOT NULL PRIMARY KEY,
    region          VARCHAR(50) NOT NULL
)
DISTSTYLE ALL
SORTKEY (region_key);

-- ---------------------------------------------------------------------------
-- Fact: orders (grain: one row per order, from orders_silver)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics.fact_orders (
    order_id        VARCHAR(20)   NOT NULL PRIMARY KEY,
    date_key        DATE          NOT NULL REFERENCES analytics.dim_date(date_key),
    customer_key    VARCHAR(20)   NOT NULL REFERENCES analytics.dim_customer(customer_key),
    product_key     VARCHAR(20)   NOT NULL REFERENCES analytics.dim_product(product_key),
    region_key      VARCHAR(50)   NOT NULL REFERENCES analytics.dim_region(region_key),
    quantity        INT           NOT NULL,
    unit_price      DECIMAL(10, 2) NOT NULL,
    discount_pct    DECIMAL(5, 2)  NOT NULL,
    total_amount    DECIMAL(12, 2) NOT NULL,
    payment_method  VARCHAR(30),
    order_status    VARCHAR(30)
)
DISTKEY (customer_key)
SORTKEY (date_key);

-- Load pattern (run from Redshift, credentials/IAM role configured AWS-side):
-- COPY analytics.fact_orders
-- FROM 's3://<bucket>/silver/orders.parquet'
-- IAM_ROLE '<redshift-iam-role-arn>'
-- FORMAT AS PARQUET;
