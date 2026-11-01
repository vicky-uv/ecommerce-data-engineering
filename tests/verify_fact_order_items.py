
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col


BASE_DIR = Path(__file__).resolve().parents[1]

ORDER_ITEMS_PATH = BASE_DIR / "data" / "curated" / "order_items"
FACT_PATH = BASE_DIR / "data" / "curated" / "facts" / "fact_order_items"


spark = (
    SparkSession.builder
    .appName("VerifyFactOrderItems")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

try:
    order_items = spark.read.parquet(str(ORDER_ITEMS_PATH))
    fact = spark.read.parquet(str(FACT_PATH))

    # Record count validation
    source_count = order_items.count()
    fact_count = fact.count()

    # Duplicate ID validation
    duplicate_count = (
        fact.groupBy("order_item_id")
        .count()
        .filter(col("count") > 1)
        .count()
    )

    # Null ID validation
    null_id_count = fact.filter(
        col("order_item_id").isNull()
    ).count()

    # Mandatory field validation
    missing_details = fact.filter(
        col("customer_id").isNull()
        | col("customer_name").isNull()
        | col("product_id").isNull()
        | col("product_name").isNull()
        | col("order_date").isNull()
    ).count()

    # Check whether any source records are missing in the fact table
    missing_source_records = (
        order_items.select("order_item_id")
        .join(
            fact.select("order_item_id"),
            on="order_item_id",
            how="left_anti"
        )
        .count()
    )

    print("\n========== FACT TABLE VALIDATION ==========")
    print(f"Source order items: {source_count}")
    print(f"Fact records: {fact_count}")
    print(f"Duplicate order item IDs: {duplicate_count}")
    print(f"Null order item IDs: {null_id_count}")
    print(f"Missing customer/product details: {missing_details}")
    print(f"Missing source records: {missing_source_records}")

    all_passed = (
        source_count == fact_count
        and duplicate_count == 0
        and null_id_count == 0
        and missing_details == 0
        and missing_source_records == 0
    )

    if not all_passed:
        raise RuntimeError("Fact order items validation failed!")

    print("\nFACT TABLE VALIDATION PASSED!")

finally:
    spark.stop()
