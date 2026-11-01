
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as spark_sum, round


BASE_DIR = Path(__file__).resolve().parents[3]
CURATED_PATH = BASE_DIR / "data" / "curated"
FACT_PATH = CURATED_PATH / "facts" / "fact_orders"


def build_fact_orders():

    spark = (
        SparkSession.builder
        .appName("BuildFactOrders")
        .master("local[*]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    try:
        print("Reading curated datasets...")

        orders = spark.read.parquet(
            str(CURATED_PATH / "orders")
        )

        customers = spark.read.parquet(
            str(CURATED_PATH / "customers")
        )

        order_items = spark.read.parquet(
            str(CURATED_PATH / "order_items")
        )

        payments = spark.read.parquet(
            str(CURATED_PATH / "payments")
        )

        print("Aggregating order items...")

        order_summary = (
            order_items
            .groupBy("order_id")
            .agg(
                count("order_item_id").alias("total_items"),
                spark_sum("quantity").alias("total_quantity"),
                round(spark_sum("item_total"), 2).alias("order_total")
            )
        )

        print("Joining order details...")

        fact = orders.join(
            order_summary,
            on="order_id",
            how="inner"
        )

        print("Joining customer details...")

        fact = fact.join(
            customers.select(
                "customer_id",
                "customer_name",
                "city"
            ),
            on="customer_id",
            how="inner"
        )

        print("Joining payment details...")

        fact = fact.join(
            payments.select(
                "order_id",
                "payment_amount",
                "payment_method",
                "payment_status"
            ),
            on="order_id",
            how="left"
        )

        fact = fact.select(
            "order_id",
            "order_date",
            "order_status",
            "customer_id",
            "customer_name",
            "city",
            "total_items",
            "total_quantity",
            "order_total",
            "payment_amount",
            "payment_method",
            "payment_status"
        )

        fact_count = fact.count()

        (
            fact.write
            .mode("overwrite")
            .parquet(str(FACT_PATH))
        )

        print("\nFact orders build completed!")
        print(f"Fact records: {fact_count}")
        print(f"Output: {FACT_PATH}")

        fact.show(5, truncate=False)

    finally:
        spark.stop()


if __name__ == "__main__":
    build_fact_orders()
