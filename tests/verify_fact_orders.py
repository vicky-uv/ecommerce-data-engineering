
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col


BASE_DIR = Path(__file__).resolve().parents[1]
CURATED_PATH = BASE_DIR / "data" / "curated"

spark = (
    SparkSession.builder
    .appName("VerifyFactOrders")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

try:
    orders = spark.read.parquet(
        str(CURATED_PATH / "orders")
    )

    fact = spark.read.parquet(
        str(CURATED_PATH / "facts" / "fact_orders")
    )

    # Record count validation
    source_count = orders.count()
    fact_count = fact.count()

    # Duplicate order ID validation
    duplicate_orders = (
        fact.groupBy("order_id")
        .count()
        .filter(col("count") > 1)
        .count()
    )

    # Null order ID validation
    null_order_ids = fact.filter(
        col("order_id").isNull()
    ).count()

    # Mandatory field validation
    missing_details = fact.filter(
        col("customer_id").isNull()
        | col("customer_name").isNull()
        | col("order_date").isNull()
        | col("order_total").isNull()
        | col("payment_amount").isNull()
    ).count()

    # Order and payment amount validation
    invalid_totals = fact.filter(
        col("order_total") != col("payment_amount")
    ).count()

    # Item count and quantity validation
    invalid_item_counts = fact.filter(
        (col("total_items").isNull())
        | (col("total_items") <= 0)
        | (col("total_quantity").isNull())
        | (col("total_quantity") <= 0)
    ).count()

    # Check for source orders missing from the fact table
    missing_source_orders = (
        orders.select("order_id")
        .join(
            fact.select("order_id"),
            on="order_id",
            how="left_anti"
        )
        .count()
    )

    print("\n========== FACT ORDERS VALIDATION ==========")
    print(f"Source orders: {source_count}")
    print(f"Fact records: {fact_count}")
    print(f"Duplicate order IDs: {duplicate_orders}")
    print(f"Null order IDs: {null_order_ids}")
    print(f"Missing mandatory details: {missing_details}")
    print(f"Order/payment total mismatches: {invalid_totals}")
    print(f"Invalid item counts or quantities: {invalid_item_counts}")
    print(f"Missing source orders: {missing_source_orders}")

    all_passed = (
        source_count == fact_count
        and duplicate_orders == 0
        and null_order_ids == 0
        and missing_details == 0
        and invalid_totals == 0
        and invalid_item_counts == 0
        and missing_source_orders == 0
    )

    if not all_passed:
        raise RuntimeError("Fact orders validation failed!")

    print("\nFACT ORDERS VALIDATION PASSED!")

finally:
    spark.stop()
