from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, initcap


BASE_DIR = Path(__file__).resolve().parents[2]

RAW_PATH = BASE_DIR / "data" / "raw" / "products"
CURATED_PATH = BASE_DIR / "data" / "curated" / "products"


def transform_products():

    spark = (
        SparkSession.builder
        .appName("TransformProducts")
        .master("local[*]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    try:
        print("Reading raw products data...")

        df = spark.read.parquet(str(RAW_PATH))

        source_count = df.count()

        # Standardize product fields
        df = (
            df
            .withColumn("product_name", trim(col("product_name")))
            .withColumn("category", initcap(trim(col("category"))))
        )

        # Validate mandatory fields and price
        df = df.filter(
            col("product_id").isNotNull()
            & (trim(col("product_id")) != "")
            & col("product_name").isNotNull()
            & (trim(col("product_name")) != "")
            & col("category").isNotNull()
            & (trim(col("category")) != "")
            & col("price").isNotNull()
            & (col("price") > 0)
        )

        # Remove duplicate product IDs
        df = df.dropDuplicates(["product_id"])

        curated_count = df.count()

        (
            df.write
            .mode("overwrite")
            .parquet(str(CURATED_PATH))
        )

        print("Product transformation completed!")
        print(f"Raw records: {source_count}")
        print(f"Curated records: {curated_count}")
        print(f"Output: {CURATED_PATH}")

    finally:
        spark.stop()


if __name__ == "__main__":
    transform_products()