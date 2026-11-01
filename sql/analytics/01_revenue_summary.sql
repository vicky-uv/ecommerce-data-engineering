
SELECT
    COUNT(DISTINCT order_id) AS total_orders,
    SUM(order_total) AS total_revenue,
    ROUND(AVG(order_total), 2) AS average_order_value,
    SUM(total_items) AS total_items_sold,
    SUM(total_quantity) AS total_units_sold
FROM read_parquet(
    'data/curated/facts/fact_orders/**/*.parquet'
);
