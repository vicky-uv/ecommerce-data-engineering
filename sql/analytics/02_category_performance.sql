
SELECT
    category,
    COUNT(DISTINCT order_id) AS total_orders,
    SUM(quantity) AS total_units_sold,
    ROUND(SUM(item_total), 2) AS total_revenue,
    ROUND(AVG(item_total), 2) AS average_item_value
FROM read_parquet(
    'data/curated/facts/fact_order_items/**/*.parquet'
)
GROUP BY category
ORDER BY total_revenue DESC;
