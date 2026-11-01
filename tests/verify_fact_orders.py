
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count


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

    source_count = orders.count()
    fact_count = fact.count()

    duplicate_orders = (
        fact.groupBy("order_id")
        .count()
        .filter(col("count") > 1)
        .count()
    )

    missing_details = fact.filter(
        col("customer_id").isNull()
        | col("customer_name").isNull()
        | col("order_date").isNull()
        | col("order_total").isNull()
        | col("payment_amount").isNull()
    ).count()

    invalid_totals = fact.filter(
        col("order_total") != col("payment_amount")
    ).count()

    invalid_item_counts = fact.filter(
        (col("total_items") <= 0)
        | (col("total_quantity") <= 0)
    ).count()

    print("\n========== FACT ORDERS VALIDATION ==========")
    print(f"Source orders: {source_count}")
    print(f"Fact records: {fact_count}")
    print(f"Duplicate order IDs: {duplicate_orders}")
    print(f"Missing mandatory details: {missing_details}")
    print(f"Order/payment total mismatches: {invalid_totals}")
    print(f"Invalid item counts or quantities: {invalid_item_counts}")

    if (
        source_count == fact_count
        and duplicate_orders == 0
        and missing_details == 0
        and invalid_totals == 0
        and invalid_item_counts == 0
    ):
        print("\nFACT ORDERS VALIDATION PASSED!")
    else:
        raise Exception("Fact orders validation failed!")

finally:
    spark.stop()
