from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round as spark_round

BASE_DIR = Path(__file__).resolve().parents[1]

CURATED_PATH = BASE_DIR / "data" / "curated" / "order_items"

spark = (
    SparkSession.builder
    .appName("VerifyCuratedOrderItems")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

df = spark.read.parquet(str(CURATED_PATH))

print("Total records:", df.count())

print(
    "Duplicate order item IDs:",
    df.count() - df.select("order_item_id").distinct().count()
)

print(
    "Invalid quantities:",
    df.filter(col("quantity").isNull() | (col("quantity") <= 0)).count()
)

print(
    "Invalid prices:",
    df.filter(col("unit_price").isNull() | (col("unit_price") <= 0)).count()
)

print(
    "Incorrect item totals:",
    df.filter(
        col("item_total") != spark_round(
            col("quantity") * col("unit_price"), 2
        )
    ).count()
)

print("\nSample curated order items:")

df.select(
    "order_item_id",
    "order_id",
    "product_id",
    "quantity",
    "unit_price",
    "item_total"
).show(5, truncate=False)

spark.stop()