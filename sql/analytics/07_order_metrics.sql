
SELECT
    order_status,
    COUNT(*) AS total_orders,
    ROUND(
        COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (),
        2
    ) AS order_percentage,
    ROUND(SUM(order_total), 2) AS total_revenue,
    ROUND(AVG(order_total), 2) AS average_order_value
FROM read_parquet(
    'data/curated/facts/fact_orders/**/*.parquet'
)
GROUP BY order_status
ORDER BY total_orders DESC;
