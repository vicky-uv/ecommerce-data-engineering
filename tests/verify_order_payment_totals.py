
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    round,
    sum as spark_sum,
)


BASE_DIR = Path(__file__).resolve().parents[1]
CURATED_PATH = BASE_DIR / "data" / "curated"

spark = (
    SparkSession.builder
    .appName("VerifyOrderPaymentTotals")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

try:
    order_items = spark.read.parquet(
        str(CURATED_PATH / "order_items")
    )

    payments = spark.read.parquet(
        str(CURATED_PATH / "payments")
    )

    # Calculate the total amount of items for each order
    item_totals = (
        order_items
        .groupBy("order_id")
        .agg(
            round(spark_sum("item_total"), 2)
            .alias("calculated_order_total")
        )
    )

    # Check for duplicate payment records
    duplicate_payments = (
        payments.groupBy("order_id")
        .agg(count("*").alias("payment_count"))
        .filter(col("payment_count") > 1)
        .count()
    )

    # Find orders without matching payment records
    missing_payments = (
        item_totals.select("order_id")
        .join(
            payments.select("order_id"),
            on="order_id",
            how="left_anti"
        )
        .count()
    )

    # Find payments without matching order items
    missing_order_items = (
        payments.select("order_id")
        .join(
            item_totals.select("order_id"),
            on="order_id",
            how="left_anti"
        )
        .count()
    )

    # Compare calculated order totals with payment amounts
    comparison = (
        item_totals
        .join(payments, on="order_id", how="inner")
        .withColumn(
            "difference",
            round(
                col("calculated_order_total") - col("payment_amount"),
                2
            )
        )
    )

    mismatches = comparison.filter(
        col("difference").isNull()
        | (col("difference") != 0)
    )

    total_orders_checked = comparison.count()
    mismatch_count = mismatches.count()

    print("\n========== ORDER PAYMENT VALIDATION ==========")
    print(f"Orders checked: {total_orders_checked}")
    print(f"Mismatched payment amounts: {mismatch_count}")
    print(f"Duplicate payment records: {duplicate_payments}")
    print(f"Orders without payments: {missing_payments}")
    print(f"Payments without order items: {missing_order_items}")

    if mismatch_count > 0:
        print("\nSample mismatches:")
        mismatches.select(
            "order_id",
            "calculated_order_total",
            "payment_amount",
            "difference"
        ).show(10, truncate=False)

    all_passed = (
        total_orders_checked > 0
        and mismatch_count == 0
        and duplicate_payments == 0
        and missing_payments == 0
        and missing_order_items == 0
    )

    if not all_passed:
        raise RuntimeError("Order payment validation failed!")

    print("\nORDER PAYMENT VALIDATION PASSED!")

finally:
    spark.stop()
