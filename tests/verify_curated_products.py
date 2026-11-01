from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim

BASE_DIR = Path(__file__).resolve().parents[1]
CURATED_PATH = BASE_DIR / "data" / "curated" / "products"

spark = (
    SparkSession.builder
    .appName("VerifyCuratedProducts")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

df = spark.read.parquet(str(CURATED_PATH))

print("Total records:", df.count())

print(
    "Duplicate product IDs:",
    df.count() - df.select("product_id").distinct().count()
)

print(
    "Invalid prices:",
    df.filter(col("price").isNull() | (col("price") <= 0)).count()
)

print(
    "Missing product names:",
    df.filter(
        col("product_name").isNull()
        | (trim(col("product_name")) == "")
    ).count()
)

print("\nSample curated products:")

df.select(
    "product_id",
    "product_name",
    "category",
    "price"
).show(5, truncate=False)

spark.stop()