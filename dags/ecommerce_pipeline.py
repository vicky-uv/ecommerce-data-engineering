# Airflow DAG definition for the e-commerce data pipeline

from datetime import datetime
from pathlib import Path
import subprocess

from airflow.sdk import dag, task


PROJECT_DIR = Path("/opt/airflow/project")


def run_script(script_path, *args):
    """Execute a project Python script and fail on errors."""
    command = [
        "python",
        str(PROJECT_DIR / script_path),
        *args,
    ]

    print(f"Running: {' '.join(command)}")

    subprocess.run(
        command,
        cwd=str(PROJECT_DIR),
        check=True,
    )


@dag(
    dag_id="ecommerce_data_pipeline",
    description="End-to-end e-commerce data engineering pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    tags=["ecommerce", "pyspark", "data-engineering"],
)
def ecommerce_pipeline():

    @task
    def ingest_customers():
        run_script("src/ingestion/ingest.py", "--dataset", "customers")

    @task
    def ingest_products():
        run_script("src/ingestion/ingest.py", "--dataset", "products")

    @task
    def ingest_orders():
        run_script("src/ingestion/ingest.py", "--dataset", "orders")

    @task
    def ingest_order_items():
        run_script("src/ingestion/ingest.py", "--dataset", "order_items")

    @task
    def ingest_payments():
        run_script("src/ingestion/ingest.py", "--dataset", "payments")

    @task
    def validate_raw():
        run_script("tests/verify_all_raw.py")

    @task
    def transform_customers():
        run_script("src/transformations/transform_customers.py")

    @task
    def transform_products():
        run_script("src/transformations/transform_products.py")

    @task
    def transform_orders():
        run_script("src/transformations/transform_orders.py")

    @task
    def transform_order_items():
        run_script("src/transformations/transform_order_items.py")

    @task
    def transform_payments():
        run_script("src/transformations/transform_payments.py")

    @task
    def validate_referential_integrity():
        run_script("tests/verify_referential_integrity.py")

    @task
    def build_fact_order_items():
        run_script("src/transformations/facts/build_fact_order_items.py")

    @task
    def build_fact_orders():
        run_script("src/transformations/facts/build_fact_orders.py")

    @task
    def validate_fact_order_items():
        run_script("tests/verify_fact_order_items.py")

    @task
    def validate_fact_orders():
        run_script("tests/verify_fact_orders.py")

    @task
    def validate_order_payment_totals():
        run_script("tests/verify_order_payment_totals.py")

    # Stage 1: Ingestion
    ingestion = [
        ingest_customers(),
        ingest_products(),
        ingest_orders(),
        ingest_order_items(),
        ingest_payments(),
    ]

    # Stage 2: Raw validation
    raw_validation = validate_raw()
    ingestion >> raw_validation

    # Stage 3: Curated transformations
    transformations = [
        transform_customers(),
        transform_products(),
        transform_orders(),
        transform_order_items(),
        transform_payments(),
    ]

    raw_validation >> transformations

    # Stage 4: Referential integrity
    integrity = validate_referential_integrity()
    transformations >> integrity

    # Stage 5: Fact table generation
    fact_items = build_fact_order_items()
    fact_orders = build_fact_orders()

    integrity >> [fact_items, fact_orders]

    # Stage 6: Final validation
    final_validations = [
        validate_fact_order_items(),
        validate_fact_orders(),
        validate_order_payment_totals(),
    ]

    fact_items >> final_validations
    fact_orders >> final_validations


ecommerce_pipeline()
