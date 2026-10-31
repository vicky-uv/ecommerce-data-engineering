from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("EcommerceDataEngineering")
    .master("local[*]")
    .getOrCreate()
)

customers_path = "data/source/customers/customers.csv"

customers_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(customers_path)
)

print("Total Customers:", customers_df.count())

customers_df.printSchema()

customers_df.show(5, truncate=False)

spark.stop()