from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, round as spark_round


BASE_DIR = Path(__file__).resolve().parents[2]

RAW_PATH = BASE_DIR / "data" / "raw" / "order_items"
CURATED_PATH = BASE_DIR / "data" / "curated" / "order_items"


def transform_order_items():

    spark = (
        SparkSession.builder
        .appName("TransformOrderItems")
        .master("local[*]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    try:
        print("Reading raw order items data...")

        df = spark.read.parquet(str(RAW_PATH))

        source_count = df.count()

        # Standardize numeric fields
        df = (
            df
            .withColumn("quantity", col("quantity").cast("integer"))
            .withColumn("unit_price", col("unit_price").cast("double"))
            .withColumn(
                "item_total",
                spark_round(col("quantity") * col("unit_price"), 2)
            )
        )

        # Validate mandatory fields
        df = df.filter(
            col("order_item_id").isNotNull()
            & (trim(col("order_item_id")) != "")
            & col("order_id").isNotNull()
            & (trim(col("order_id")) != "")
            & col("product_id").isNotNull()
            & (trim(col("product_id")) != "")
            & col("quantity").isNotNull()
            & (col("quantity") > 0)
            & col("unit_price").isNotNull()
            & (col("unit_price") > 0)
        )

        # Remove duplicate order item IDs
        df = df.dropDuplicates(["order_item_id"])

        curated_count = df.count()

        (
            df.write
            .mode("overwrite")
            .parquet(str(CURATED_PATH))
        )

        print("Order items transformation completed!")
        print(f"Raw records: {source_count}")
        print(f"Curated records: {curated_count}")
        print(f"Output: {CURATED_PATH}")

    finally:
        spark.stop()


if __name__ == "__main__":
    transform_order_items()