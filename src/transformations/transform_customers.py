
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, lower, initcap


BASE_DIR = Path(__file__).resolve().parents[2]

RAW_PATH = BASE_DIR / "data" / "raw" / "customers"
CURATED_PATH = BASE_DIR / "data" / "curated" / "customers"


def transform_customers():

    spark = (
        SparkSession.builder
        .appName("TransformCustomers")
        .master("local[*]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    try:
        print("Reading raw customers data...")

        df = spark.read.parquet(str(RAW_PATH))

        source_count = df.count()

        # Standardize customer fields
        df = (
            df
            .withColumn("customer_name", trim(col("customer_name")))
            .withColumn("email", lower(trim(col("email"))))
            .withColumn("city", initcap(trim(col("city"))))
        )

        # Remove records with missing mandatory fields
        df = df.filter(
            col("customer_id").isNotNull()
            & (trim(col("customer_id")) != "")
            & col("customer_name").isNotNull()
            & (col("customer_name") != "")
            & col("email").isNotNull()
            & (col("email") != "")
        )

        # Remove duplicate customer records
        df = df.dropDuplicates(["customer_id"])

        curated_count = df.count()

        # Write curated data
        (
            df.write
            .mode("overwrite")
            .parquet(str(CURATED_PATH))
        )

        print("Customer transformation completed!")
        print(f"Raw records: {source_count}")
        print(f"Curated records: {curated_count}")
        print(f"Output: {CURATED_PATH}")

    finally:
        spark.stop()


if __name__ == "__main__":
    transform_customers()
