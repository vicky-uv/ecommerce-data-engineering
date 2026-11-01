from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, trim

BASE_DIR = Path(__file__).resolve().parents[1]
CURATED_PATH = BASE_DIR / "data" / "curated" / "customers"

spark = (
    SparkSession.builder
    .appName("VerifyCuratedCustomers")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

df = spark.read.parquet(str(CURATED_PATH))

print("Total records:", df.count())

print("Duplicate customer IDs:", 
      df.count() - df.select("customer_id").distinct().count())

print("Null customer IDs:", 
      df.filter(col("customer_id").isNull()).count())

print("Emails not in lowercase:", 
      df.filter(col("email") != lower(trim(col("email")))).count())

print("\nSample curated records:")
df.select(
    "customer_id",
    "customer_name",
    "email",
    "city"
).show(5, truncate=False)

spark.stop()