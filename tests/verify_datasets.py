
import pandas as pd
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
SOURCE_DIR = BASE_DIR / "data" / "source"

# Load datasets
customers = pd.read_csv(SOURCE_DIR / "customers/customers.csv")
products = pd.read_csv(SOURCE_DIR / "products/products.csv")
orders = pd.read_csv(SOURCE_DIR / "orders/orders.csv")
items = pd.read_csv(SOURCE_DIR / "order_items/order_items.csv")
payments = pd.read_csv(SOURCE_DIR / "payments/payments.csv")


# 1. Record count validation
print("\n--- RECORD COUNTS ---")

datasets = {
    "Customers": customers,
    "Products": products,
    "Orders": orders,
    "Order Items": items,
    "Payments": payments
}

for name, df in datasets.items():
    print(f"{name}: {len(df)} records")


# 2. Primary key validation
print("\n--- PRIMARY KEY VALIDATION ---")

primary_keys = {
    "Customers": (customers, "customer_id"),
    "Products": (products, "product_id"),
    "Orders": (orders, "order_id"),
    "Order Items": (items, "order_item_id"),
    "Payments": (payments, "payment_id")
}

for name, (df, column) in primary_keys.items():
    duplicates = df[column].duplicated().sum()
    print(f"{name} duplicate keys: {duplicates}")


# 3. Foreign key validation
print("\n--- FOREIGN KEY VALIDATION ---")

invalid_customers = (
    ~orders["customer_id"].isin(customers["customer_id"])
).sum()

invalid_products = (
    ~items["product_id"].isin(products["product_id"])
).sum()

invalid_orders_in_items = (
    ~items["order_id"].isin(orders["order_id"])
).sum()

invalid_orders_in_payments = (
    ~payments["order_id"].isin(orders["order_id"])
).sum()

print("Invalid customer references:", invalid_customers)
print("Invalid product references:", invalid_products)
print("Invalid order references in items:", invalid_orders_in_items)
print("Invalid order references in payments:", invalid_orders_in_payments)


# 4. Null validation
print("\n--- NULL VALIDATION ---")

for name, df in datasets.items():
    print(f"\n{name}")
    print(df.isnull().sum())


# 5. Payment relationship validation
print("\n--- PAYMENT VALIDATION ---")

duplicate_payments = payments["order_id"].duplicated().sum()

print("Orders with multiple payment records:", duplicate_payments)

# 6. Final result
print("\n--- VALIDATION COMPLETED ---")
print("Dataset verification script executed successfully!")
