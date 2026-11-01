from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count


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

    source_count = order_items.count()
    fact_count = fact.count()

    duplicate_count = (
        fact.groupBy("order_item_id")
        .count()
        .filter(col("count") > 1)
        .count()
    )

    missing_details = fact.filter(
        col("customer_id").isNull()
        | col("customer_name").isNull()
        | col("product_id").isNull()
        | col("product_name").isNull()
        | col("order_date").isNull()
    ).count()

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
    print(f"Missing customer/product details: {missing_details}")
    print(f"Missing source records: {missing_source_records}")

    if (
        source_count == fact_count
        and duplicate_count == 0
        and missing_details == 0
        and missing_source_records == 0
    ):
        print("\nFACT TABLE VALIDATION PASSED!")
    else:
        raise Exception("Fact table validation failed!")

finally:
    spark.stop()