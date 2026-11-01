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

# Read curated datasets
customers = spark.read.parquet(str(CURATED_PATH / "customers"))
products = spark.read.parquet(str(CURATED_PATH / "products"))
orders = spark.read.parquet(str(CURATED_PATH / "orders"))
order_items = spark.read.parquet(str(CURATED_PATH / "order_items"))
payments = spark.read.parquet(str(CURATED_PATH / "payments"))

print("\n========== REFERENTIAL INTEGRITY VALIDATION ==========\n")

# Orders -> Customers
invalid_order_customers = (
    orders.alias("o")
    .join(
        customers.select("customer_id").alias("c"),
        "customer_id",
        "left_anti"
    )
)

# Order items -> Orders
invalid_item_orders = (
    order_items.alias("oi")
    .join(
        orders.select("order_id").alias("o"),
        "order_id",
        "left_anti"
    )
)

# Order items -> Products
invalid_item_products = (
    order_items.alias("oi")
    .join(
        products.select("product_id").alias("p"),
        "product_id",
        "left_anti"
    )
)

# Payments -> Orders
invalid_payment_orders = (
    payments.alias("p")
    .join(
        orders.select("order_id").alias("o"),
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

for check_name, invalid_count in checks.items():
    status = "PASSED" if invalid_count == 0 else "FAILED"

    print(f"{check_name}: {invalid_count} | {status}")

print("\n====================================================")

if all(count == 0 for count in checks.values()):
    print("ALL REFERENTIAL INTEGRITY CHECKS PASSED!")
else:
    print("REFERENTIAL INTEGRITY ISSUES FOUND!")

spark.stop()