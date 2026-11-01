from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, round


BASE_DIR = Path(__file__).resolve().parents[1]
CURATED_PATH = BASE_DIR / "data" / "curated"

spark = (
    SparkSession.builder
    .appName("VerifyOrderPaymentTotals")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

try:
    order_items = spark.read.parquet(
        str(CURATED_PATH / "order_items")
    )

    payments = spark.read.parquet(
        str(CURATED_PATH / "payments")
    )

    item_totals = (
        order_items
        .groupBy("order_id")
        .agg(
            round(spark_sum("item_total"), 2)
            .alias("calculated_order_total")
        )
    )

    comparison = (
        item_totals
        .join(payments, on="order_id", how="inner")
        .withColumn(
            "difference",
            round(
                col("calculated_order_total") - col("payment_amount"),
                2
            )
        )
    )

    mismatches = comparison.filter(
        col("difference") != 0
    )

    print("Total orders checked:", comparison.count())
    print("Mismatched payment amounts:", mismatches.count())

    print("\nSample mismatches:")
    mismatches.select(
        "order_id",
        "calculated_order_total",
        "payment_amount",
        "difference"
    ).show(10, truncate=False)

finally:
    spark.stop()