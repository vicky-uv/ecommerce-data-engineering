
SELECT
    city,
    COUNT(DISTINCT order_id) AS total_orders,
    COUNT(DISTINCT customer_id) AS unique_customers,
    ROUND(SUM(order_total), 2) AS total_revenue,
    ROUND(AVG(order_total), 2) AS average_order_value
FROM read_parquet(
    'data/curated/facts/fact_orders/**/*.parquet'
)
GROUP BY city
ORDER BY total_revenue DESC;
