
import argparse
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, current_timestamp, lit


BASE_DIR = Path(__file__).resolve().parents[2]

DATASETS = {
    "customers": [
        "customer_id",
        "customer_name",
        "email",
        "city",
        "registration_date",
    ],
    "products": [
        "product_id",
        "product_name",
        "category",
        "price",
    ],
    "orders": [
        "order_id",
        "customer_id",
        "order_date",
        "order_status",
    ],
    "order_items": [
        "order_item_id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        "item_total",
    ],
    "payments": [
        "payment_id",
        "order_id",
        "payment_method",
        "payment_amount",
        "payment_status",
    ],
}


def ingest_dataset(dataset_name):
    if dataset_name not in DATASETS:
        raise ValueError(f"Unsupported dataset: {dataset_name}")

    input_path = (
        BASE_DIR / "data" / "source" /
        dataset_name / f"{dataset_name}.csv"
    )

    output_path = BASE_DIR / "data" / "raw" / dataset_name

    if not input_path.exists():
        raise FileNotFoundError(f"Source file not found: {input_path}")

    batch_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    spark = (
        SparkSession.builder
        .appName(f"EcommerceIngestion-{dataset_name}")
        .master("local[*]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    try:
        print(f"Starting ingestion: {dataset_name}")
        print(f"Batch ID: {batch_id}")

        df = (
            spark.read
            .option("header", True)
            .option("inferSchema", True)
            .csv(str(input_path))
        )

        # Validate the source schema
        expected_columns = set(DATASETS[dataset_name])
        actual_columns = set(df.columns)

        missing_columns = expected_columns - actual_columns

        if missing_columns:
            raise ValueError(
                f"Missing columns in {dataset_name}: {missing_columns}"
            )

        # Add ingestion metadata
        df = (
            df
            .withColumn("ingestion_timestamp", current_timestamp())
            .withColumn("batch_id", lit(batch_id))
            .withColumn("source_file", lit(input_path.name))
            .withColumn("ingestion_date", current_date())
        )

        record_count = df.count()

        # Write immutable batch output to the raw layer
        (
            df.write
            .mode("append")
            .partitionBy("ingestion_date", "batch_id")
            .parquet(str(output_path))
        )

        print(f"Successfully ingested {record_count} records")
        print(f"Raw output location: {output_path}")

    finally:
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="E-commerce data ingestion framework"
    )

    parser.add_argument(
        "--dataset",
        required=True,
        choices=DATASETS.keys(),
        help="Dataset to ingest",
    )

    args = parser.parse_args()

    ingest_dataset(args.dataset)
