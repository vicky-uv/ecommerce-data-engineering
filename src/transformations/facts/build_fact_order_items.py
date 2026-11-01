from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col


BASE_DIR = Path(__file__).resolve().parents[3]
CURATED_PATH = BASE_DIR / "data" / "curated"
FACT_PATH = BASE_DIR / "data" / "curated" / "facts" / "fact_order_items"


def build_fact_order_items():

    spark = (
        SparkSession.builder
        .appName("BuildFactOrderItems")
        .master("local[*]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    try:
        print("Reading curated datasets...")

        customers = spark.read.parquet(
            str(CURATED_PATH / "customers")
        )

        products = spark.read.parquet(
            str(CURATED_PATH / "products")
        )

        orders = spark.read.parquet(
            str(CURATED_PATH / "orders")
        )

        order_items = spark.read.parquet(
            str(CURATED_PATH / "order_items")
        )

        payments = spark.read.parquet(
            str(CURATED_PATH / "payments")
        )

        print("Joining order items with orders...")

        fact = order_items.join(
            orders.select(
                "order_id",
                "customer_id",
                "order_date",
                "order_status"
            ),
            on="order_id",
            how="inner"
        )

        print("Joining customer details...")

        fact = fact.join(
            customers.select(
                "customer_id",
                "customer_name",
                "email",
                "city"
            ),
            on="customer_id",
            how="inner"
        )

        print("Joining product details...")

        fact = fact.join(
            products.select(
                "product_id",
                "product_name",
                "category"
            ),
            on="product_id",
            how="inner"
        )

        print("Joining payment details...")

        fact = fact.join(
            payments.select(
                "order_id",
                "payment_method",
                "payment_status"
            ),
            on="order_id",
            how="left"
        )

        # Select final analytics columns
        fact = fact.select(
            "order_item_id",
            "order_id",
            "order_date",
            "order_status",
            "customer_id",
            "customer_name",
            "email",
            "city",
            "product_id",
            "product_name",
            "category",
            "quantity",
            "unit_price",
            "item_total",
            "payment_method",
            "payment_status"
        )

        fact_count = fact.count()

        (
            fact.write
            .mode("overwrite")
            .parquet(str(FACT_PATH))
        )

        print("\nFact table build completed!")
        print(f"Fact records: {fact_count}")
        print(f"Output: {FACT_PATH}")

        fact.show(5, truncate=False)

    finally:
        spark.stop()


if __name__ == "__main__":
    build_fact_order_items()