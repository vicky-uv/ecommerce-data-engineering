from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

BASE_DIR = Path(__file__).resolve().parents[1]
RAW_PATH = BASE_DIR / "data" / "raw" / "customers"

spark = (
    SparkSession.builder
    .appName("VerifyRawCustomers")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

try:
    df = spark.read.parquet(str(RAW_PATH))

    print("\n--- RAW CUSTOMERS VALIDATION ---")

    total_records = df.count()
    print("Total records:", total_records)

    print("Columns:", df.columns)

    print("Unique batch IDs:", df.select("batch_id").distinct().count())

    print(
        "Null ingestion timestamps:",
        df.filter(col("ingestion_timestamp").isNull()).count()
    )

    print(
        "Source files:",
        df.select("source_file").distinct().collect()
    )

    print("\nSample records:")
    df.show(5, truncate=False)

    assert total_records == 10000, "Record count mismatch!"
    assert "ingestion_timestamp" in df.columns
    assert "batch_id" in df.columns
    assert "ingestion_date" in df.columns

    print("\nVALIDATION PASSED!")

finally:
    spark.stop()