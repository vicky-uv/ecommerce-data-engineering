
import csv
import random
from pathlib import Path
from datetime import datetime, timedelta

# Reproducible random data
random.seed(42)

BASE_DIR = Path(__file__).resolve().parents[1]
SOURCE_DIR = BASE_DIR / "data" / "source"


def write_csv(folder, filename, rows):
    path = SOURCE_DIR / folder / filename
    path.parent.mkdir(parents=True, exist_ok=True)

    with open(path, "w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)

    print(f"Created {filename}: {len(rows)} records")


# --------------------------------------------------
# 1. CUSTOMERS DATASET
# --------------------------------------------------

customers = []

cities = [
    "Bangalore", "Chennai", "Mumbai", "Delhi",
    "Hyderabad", "Pune", "Kolkata", "Coimbatore"
]

for i in range(1, 10001):
    customers.append({
        "customer_id": f"C{i:06d}",
        "customer_name": f"Customer_{i}",
        "email": f"customer{i}@example.com",
        "city": random.choice(cities),
        "registration_date": (
            datetime(2024, 1, 1)
            + timedelta(days=random.randint(0, 1000))
        ).date().isoformat()
    })

write_csv("customers", "customers.csv", customers)


# --------------------------------------------------
# 2. PRODUCTS DATASET
# --------------------------------------------------

products = []

categories = [
    "Electronics", "Fashion", "Home",
    "Books", "Sports", "Beauty"
]

for i in range(1, 2001):
    products.append({
        "product_id": f"P{i:05d}",
        "product_name": f"Product_{i}",
        "category": random.choice(categories),
        "price": round(random.uniform(100, 50000), 2)
    })

write_csv("products", "products.csv", products)


# --------------------------------------------------
# 3. ORDERS, ORDER ITEMS AND PAYMENTS
# --------------------------------------------------

orders = []
order_items = []
payments = []

payment_methods = [
    "UPI", "CARD", "NETBANKING", "COD"
]

order_statuses = [
    "PLACED", "SHIPPED", "DELIVERED", "CANCELLED"
]

start_date = datetime(2025, 1, 1)

for i in range(1, 50001):

    order_id = f"O{i:07d}"
    customer = random.choice(customers)

    order_date = start_date + timedelta(
        days=random.randint(0, 668),
        seconds=random.randint(0, 86399)
    )

    order_status = random.choice(order_statuses)

    # Orders dataset
    orders.append({
        "order_id": order_id,
        "customer_id": customer["customer_id"],
        "order_date": order_date.isoformat(),
        "order_status": order_status
    })

    # Order Items dataset
    item_count = random.randint(1, 4)
    order_total = 0

    for j in range(1, item_count + 1):

        product = random.choice(products)
        quantity = random.randint(1, 3)
        unit_price = product["price"]

        item_total = round(quantity * unit_price, 2)
        order_total += item_total

        order_items.append({
            "order_item_id": f"OI{i:07d}_{j}",
            "order_id": order_id,
            "product_id": product["product_id"],
            "quantity": quantity,
            "unit_price": unit_price,
            "item_total": item_total
        })

    # Payments dataset
    payments.append({
        "payment_id": f"PAY{i:07d}",
        "order_id": order_id,
        "payment_method": random.choice(payment_methods),
        "payment_amount": round(order_total, 2),
        "payment_status": random.choice(
            ["SUCCESS", "FAILED", "PENDING"]
        )
    })


# --------------------------------------------------
# 4. WRITE ALL DATASETS
# --------------------------------------------------

write_csv("orders", "orders.csv", orders)

write_csv(
    "order_items",
    "order_items.csv",
    order_items
)

write_csv(
    "payments",
    "payments.csv",
    payments
)

print("\nAll source datasets generated successfully!")
