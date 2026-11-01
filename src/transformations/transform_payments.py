from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, upper, round as spark_round


BASE_DIR = Path(__file__).resolve().parents[2]

RAW_PATH = BASE_DIR / "data" / "raw" / "payments"
CURATED_PATH = BASE_DIR / "data" / "curated" / "payments"


def transform_payments():

    spark = (
        SparkSession.builder
        .appName("TransformPayments")
        .master("local[*]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    try:
        print("Reading raw payments data...")

        df = spark.read.parquet(str(RAW_PATH))

        source_count = df.count()

        # Standardize payment fields
        df = (
            df
            .withColumn("payment_method", upper(trim(col("payment_method"))))
            .withColumn("payment_status", upper(trim(col("payment_status"))))
            .withColumn(
                "payment_amount",
                spark_round(col("payment_amount").cast("double"), 2)
            )
        )

        # Validate mandatory fields
        df = df.filter(
            col("payment_id").isNotNull()
            & (trim(col("payment_id")) != "")
            & col("order_id").isNotNull()
            & (trim(col("order_id")) != "")
            & col("payment_method").isNotNull()
            & (col("payment_method") != "")
            & col("payment_amount").isNotNull()
            & (col("payment_amount") > 0)
            & col("payment_status").isNotNull()
            & (col("payment_status") != "")
        )

        # Remove duplicate payment IDs
        df = df.dropDuplicates(["payment_id"])

        curated_count = df.count()

        (
            df.write
            .mode("overwrite")
            .parquet(str(CURATED_PATH))
        )

        print("Payment transformation completed!")
        print(f"Raw records: {source_count}")
        print(f"Curated records: {curated_count}")
        print(f"Output: {CURATED_PATH}")

    finally:
        spark.stop()


if __name__ == "__main__":
    transform_payments()