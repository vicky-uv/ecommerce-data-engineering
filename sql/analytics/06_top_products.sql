
SELECT
    product_id,
    product_name,
    category,
    SUM(quantity) AS total_units_sold,
    COUNT(DISTINCT order_id) AS total_orders,
    ROUND(SUM(item_total), 2) AS total_revenue
FROM read_parquet(
    'data/curated/facts/fact_order_items/**/*.parquet'
)
GROUP BY
    product_id,
    product_name,
    category
ORDER BY
    total_revenue DESC
LIMIT 10;
