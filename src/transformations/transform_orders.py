from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, to_date


BASE_DIR = Path(__file__).resolve().parents[2]

RAW_PATH = BASE_DIR / "data" / "raw" / "orders"
CURATED_PATH = BASE_DIR / "data" / "curated" / "orders"


def transform_orders():

    spark = (
        SparkSession.builder
        .appName("TransformOrders")
        .master("local[*]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    try:
        print("Reading raw orders data...")

        df = spark.read.parquet(str(RAW_PATH))

        source_count = df.count()

        # Standardize order fields
        df = (
            df
            .withColumn("order_status", trim(col("order_status")))
            .withColumn("order_date", to_date(col("order_date")))
        )

        # Validate mandatory fields
        df = df.filter(
            col("order_id").isNotNull()
            & (trim(col("order_id")) != "")
            & col("customer_id").isNotNull()
            & (trim(col("customer_id")) != "")
            & col("order_date").isNotNull()
            & col("order_status").isNotNull()
            & (col("order_status") != "")
        )

        # Remove duplicate orders
        df = df.dropDuplicates(["order_id"])

        curated_count = df.count()

        (
            df.write
            .mode("overwrite")
            .parquet(str(CURATED_PATH))
        )

        print("Order transformation completed!")
        print(f"Raw records: {source_count}")
        print(f"Curated records: {curated_count}")
        print(f"Output: {CURATED_PATH}")

    finally:
        spark.stop()


if __name__ == "__main__":
    transform_orders()