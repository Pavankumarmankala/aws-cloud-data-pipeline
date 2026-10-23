-- Athena queries for the AWS Cloud Data Pipeline
-- Database: aws_cloud_pipeline
-- Tables:
--   orders_silver        -> data/silver/orders.parquet (partitioned by region)
--   daily_revenue, revenue_by_category, revenue_by_region, top_products,
--   customer_summary, monthly_summary  -> data/gold/*.parquet (Glue output)
-- Athena uses Presto/Trino SQL. Run after the Glue crawler has catalogued
-- the silver and gold S3 locations.

-- Query 1: Total revenue and order volume
SELECT COUNT(*) AS total_orders,
       SUM(total_amount) AS total_revenue,
       AVG(total_amount) AS avg_order_value
FROM aws_cloud_pipeline.orders_silver;

-- Query 2: Daily revenue trend
SELECT order_date,
       COUNT(*) AS orders,
       SUM(total_amount) AS revenue
FROM aws_cloud_pipeline.orders_silver
GROUP BY order_date
ORDER BY order_date;

-- Query 3: Revenue by category
SELECT category,
       COUNT(*) AS orders,
       SUM(total_amount) AS revenue,
       100.0 * SUM(total_amount) / SUM(SUM(total_amount)) OVER () AS revenue_share_pct
FROM aws_cloud_pipeline.orders_silver
GROUP BY category
ORDER BY revenue DESC;

-- Query 4: Revenue by region
SELECT region,
       COUNT(*) AS orders,
       COUNT(DISTINCT customer_id) AS unique_customers,
       SUM(total_amount) AS revenue
FROM aws_cloud_pipeline.orders_silver
GROUP BY region
ORDER BY revenue DESC;

-- Query 5: Top 10 products by revenue
SELECT product_id, product_name, category, orders, revenue, units_sold
FROM aws_cloud_pipeline.top_products
ORDER BY revenue DESC
LIMIT 10;

-- Query 6: Monthly revenue summary
SELECT year_month, orders, revenue, unique_customers, avg_order_value
FROM aws_cloud_pipeline.monthly_summary
ORDER BY year_month;

-- Query 7: Month-over-month revenue growth
SELECT year_month,
       revenue,
       LAG(revenue) OVER (ORDER BY year_month) AS prev_month_revenue,
       100.0 * (revenue - LAG(revenue) OVER (ORDER BY year_month))
           / LAG(revenue) OVER (ORDER BY year_month) AS growth_pct
FROM aws_cloud_pipeline.monthly_summary
ORDER BY year_month;

-- Query 8: Top 20 customers by total spend
SELECT customer_id, orders, total_spend, units_bought, first_order, last_order
FROM aws_cloud_pipeline.customer_summary
ORDER BY total_spend DESC
LIMIT 20;

-- Query 9: Average order value by category and region
SELECT category, region,
       COUNT(*) AS orders,
       AVG(total_amount) AS avg_order_value
FROM aws_cloud_pipeline.orders_silver
GROUP BY category, region
ORDER BY category, avg_order_value DESC;

-- Query 10: Payment method breakdown
SELECT payment_method,
       COUNT(*) AS orders,
       SUM(total_amount) AS revenue,
       AVG(total_amount) AS avg_order_value
FROM aws_cloud_pipeline.orders_silver
GROUP BY payment_method
ORDER BY revenue DESC;

-- Query 11: Discount impact on revenue
SELECT discount_pct,
       COUNT(*) AS orders,
       SUM(quantity) AS units_sold,
       SUM(total_amount) AS revenue,
       AVG(total_amount) AS avg_order_value
FROM aws_cloud_pipeline.orders_silver
GROUP BY discount_pct
ORDER BY discount_pct;

-- Query 12: Repeat customers (more than one order)
SELECT COUNT(*) AS repeat_customers,
       SUM(orders) AS repeat_orders,
       SUM(total_spend) AS revenue_from_repeat_customers
FROM aws_cloud_pipeline.customer_summary
WHERE orders > 1;
