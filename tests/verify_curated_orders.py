from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim

BASE_DIR = Path(__file__).resolve().parents[1]
CURATED_PATH = BASE_DIR / "data" / "curated" / "orders"

spark = (
    SparkSession.builder
    .appName("VerifyCuratedOrders")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

df = spark.read.parquet(str(CURATED_PATH))

print("Total records:", df.count())

print(
    "Duplicate order IDs:",
    df.count() - df.select("order_id").distinct().count()
)

print(
    "Missing customer IDs:",
    df.filter(
        col("customer_id").isNull()
        | (trim(col("customer_id")) == "")
    ).count()
)

print(
    "Missing order dates:",
    df.filter(col("order_date").isNull()).count()
)

print("\nOrder status distribution:")
df.groupBy("order_status").count().show()

print("\nSample curated orders:")
df.select(
    "order_id",
    "customer_id",
    "order_date",
    "order_status"
).show(5, truncate=False)

spark.stop()