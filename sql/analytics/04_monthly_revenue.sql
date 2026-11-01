
SELECT
    DATE_TRUNC('month', order_date) AS order_month,
    COUNT(DISTINCT order_id) AS total_orders,
    ROUND(SUM(order_total), 2) AS total_revenue,
    ROUND(AVG(order_total), 2) AS average_order_value
FROM read_parquet(
    'data/curated/facts/fact_orders/**/*.parquet'
)
GROUP BY DATE_TRUNC('month', order_date)
ORDER BY order_month;
