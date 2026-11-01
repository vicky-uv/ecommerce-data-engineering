
from pathlib import Path
from pyspark.sql import SparkSession

BASE_DIR = Path(__file__).resolve().parents[1]
CURATED_PATH = BASE_DIR / "data" / "curated"

spark = (
    SparkSession.builder
    .appName("ReferentialIntegrityValidation")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

try:
    # Read curated datasets
    customers = spark.read.parquet(str(CURATED_PATH / "customers"))
    products = spark.read.parquet(str(CURATED_PATH / "products"))
    orders = spark.read.parquet(str(CURATED_PATH / "orders"))
    order_items = spark.read.parquet(str(CURATED_PATH / "order_items"))
    payments = spark.read.parquet(str(CURATED_PATH / "payments"))

    print("\n========== REFERENTIAL INTEGRITY VALIDATION ==========\n")

    # Orders -> Customers
    invalid_order_customers = (
        orders.join(
            customers.select("customer_id"),
            "customer_id",
            "left_anti"
        )
    )

    # Order items -> Orders
    invalid_item_orders = (
        order_items.join(
            orders.select("order_id"),
            "order_id",
            "left_anti"
        )
    )

    # Order items -> Products
    invalid_item_products = (
        order_items.join(
            products.select("product_id"),
            "product_id",
            "left_anti"
        )
    )

    # Payments -> Orders
    invalid_payment_orders = (
        payments.join(
            orders.select("order_id"),
            "order_id",
            "left_anti"
        )
    )

    checks = {
        "Orders with invalid customers": invalid_order_customers.count(),
        "Order items with invalid orders": invalid_item_orders.count(),
        "Order items with invalid products": invalid_item_products.count(),
        "Payments with invalid orders": invalid_payment_orders.count(),
    }

    all_passed = True

    for check_name, invalid_count in checks.items():
        status = "PASSED" if invalid_count == 0 else "FAILED"

        print(f"{check_name}: {invalid_count} | {status}")

        if invalid_count != 0:
            all_passed = False

    print("\n====================================================")

    if not all_passed:
        raise RuntimeError("Referential integrity validation failed!")

    print("ALL REFERENTIAL INTEGRITY CHECKS PASSED!")

finally:
    spark.stop()
