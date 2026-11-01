from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, upper

BASE_DIR = Path(__file__).resolve().parents[1]
CURATED_PATH = BASE_DIR / "data" / "curated" / "payments"

spark = (
    SparkSession.builder
    .appName("VerifyCuratedPayments")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

df = spark.read.parquet(str(CURATED_PATH))

print("Total records:", df.count())

print(
    "Duplicate payment IDs:",
    df.count() - df.select("payment_id").distinct().count()
)

print(
    "Invalid payment amounts:",
    df.filter(
        col("payment_amount").isNull()
        | (col("payment_amount") <= 0)
    ).count()
)

print(
    "Payment methods not uppercase:",
    df.filter(
        col("payment_method") != upper(trim(col("payment_method")))
    ).count()
)

print("\nPayment status distribution:")
df.groupBy("payment_status").count().show()

print("\nSample payments:")
df.show(5, truncate=False)

spark.stop()