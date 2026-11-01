
SELECT
    payment_method,
    payment_status,
    COUNT(*) AS total_transactions,
    ROUND(SUM(payment_amount), 2) AS total_payment_amount,
    ROUND(AVG(payment_amount), 2) AS average_payment_amount
FROM read_parquet(
    'data/curated/facts/fact_orders/**/*.parquet'
)
GROUP BY
    payment_method,
    payment_status
ORDER BY
    payment_method,
    payment_status;
