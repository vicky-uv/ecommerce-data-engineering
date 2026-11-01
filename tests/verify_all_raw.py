
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import max as spark_max, col

BASE_DIR = Path(__file__).resolve().parents[1]

EXPECTED_COUNTS = {
    "customers": 10000,
    "products": 2000,
    "orders": 50000,
    "order_items": 124949,
    "payments": 50000,
}

spark = (
    SparkSession.builder
    .appName("VerifyAllRawDatasets")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

try:
    all_passed = True

    print("\n========== RAW DATA VALIDATION ==========")

    for dataset, expected_count in EXPECTED_COUNTS.items():
        path = BASE_DIR / "data" / "raw" / dataset

        try:
            df = spark.read.parquet(str(path))

            # Identify the most recent ingestion batch
            latest_batch = (
                df.select(spark_max("batch_id").alias("latest_batch"))
                .first()["latest_batch"]
            )

            if latest_batch is None:
                raise ValueError("No ingestion batch found")

            # Validate records belonging to that batch only
            latest_df = df.filter(col("batch_id") == latest_batch)
            actual_count = latest_df.count()

            status = (
                "PASSED"
                if actual_count == expected_count
                else "FAILED"
            )

            print(
                f"{dataset:15} | "
                f"Expected: {expected_count:7} | "
                f"Actual: {actual_count:7} | "
                f"Batch: {latest_batch} | {status}"
            )

            if actual_count != expected_count:
                all_passed = False

        except Exception as error:
            print(f"{dataset:15} | FAILED | {error}")
            all_passed = False

    print("\n=========================================")

    if all_passed:
        print("ALL DATASETS VALIDATED!")
    else:
        raise RuntimeError("Raw dataset validation failed!")

finally:
    spark.stop()
