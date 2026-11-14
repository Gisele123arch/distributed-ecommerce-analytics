
import os
import sys
from pathlib import Path

# Configure Spark environment
PROJECT_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_DIR / "data"
OUTPUT_DIR = PROJECT_DIR / "outputs"

SPARK_HOME = Path(
    r"C:\Users\i7\Downloads\spark-3.5.9-bin-hadoop3"
    r"\spark-3.5.9-bin-hadoop3"
)

os.environ["JAVA_HOME"] = r"C:\Program Files\Java\jdk-17.0.20.1"
os.environ["SPARK_HOME"] = str(SPARK_HOME)
os.environ["PYSPARK_PYTHON"] = "python"
os.environ["PYSPARK_DRIVER_PYTHON"] = "python"

sys.path.insert(0, str(SPARK_HOME / "python" / "lib" / "pyspark.zip"))
sys.path.insert(0, str(SPARK_HOME / "python" / "lib" / "py4j-0.10.9.7-src.zip"))

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, explode, sum as spark_sum, countDistinct,
    count, round as spark_round, desc, collect_set,
    to_date, date_format
)

def local_file(filename):
    path = (DATA_DIR / filename).resolve()
    return "file:///" + str(path).replace("\\", "/")

def main():
    OUTPUT_DIR.mkdir(exist_ok=True)

    spark = (
        SparkSession.builder
        .appName("EcommerceIntegratedAnalytics")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.hadoop.fs.defaultFS", "file:///")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    print("\n========== STEP 9: INTEGRATED ANALYTICS ==========")

    # 1. Load source datasets
    users = spark.read.option("multiline", "true").json(local_file("users.json"))
    products = spark.read.option("multiline", "true").json(local_file("products.json"))
    transactions = spark.read.option("multiline", "true").json(local_file("transactions.json"))
    sessions = spark.read.option("multiline", "true").json(local_file("sessions_0.json"))

    # 2. Select completed transactions
    delivered = transactions.filter(col("status") == "delivered")

    # 3. Expand purchased items
    items = (
        delivered
        .select("transaction_id", "user_id", "timestamp", explode("items").alias("item"))
        .select(
            "transaction_id",
            "user_id",
            "timestamp",
            col("item.product_id").alias("product_id"),
            col("item.quantity").alias("quantity"),
            col("item.subtotal").alias("item_revenue")
        )
    )

    # 4. Customer lifetime value (based on available transaction history)
    customer_value = (
        delivered.groupBy("user_id")
        .agg(
            countDistinct("transaction_id").alias("purchase_count"),
            spark_round(spark_sum("total"), 2).alias("lifetime_value")
        )
        .join(users.select("user_id"), "user_id", "left")
        .orderBy(desc("lifetime_value"))
    )

    print("\n========== CUSTOMER LIFETIME VALUE ==========")
    customer_value.show(10, truncate=False)

    # 5. Product affinity: products bought together
    first_product = items.alias("a")
    second_product = items.alias("b")

    product_affinity = (
        first_product.join(
            second_product,
            (col("a.transaction_id") == col("b.transaction_id")) &
            (col("a.product_id") < col("b.product_id")),
            "inner"
        )
        .groupBy(
            col("a.product_id").alias("product_1"),
            col("b.product_id").alias("product_2")
        )
        .agg(countDistinct(col("a.transaction_id")).alias("times_bought_together"))
        .orderBy(desc("times_bought_together"))
    )

    product_affinity = (
        product_affinity
        .join(
            products.select(
                col("product_id").alias("product_1"),
                col("name").alias("product_1_name")
            ),
            "product_1",
            "left"
        )
        .join(
            products.select(
                col("product_id").alias("product_2"),
                col("name").alias("product_2_name")
            ),
            "product_2",
            "left"
        )
    )

    print("\n========== PRODUCT AFFINITY ==========")
    product_affinity.show(10, truncate=False)

    # 6. Conversion analysis from session activity
    funnel = (
        sessions.groupBy("conversion_status")
        .agg(count("*").alias("session_count"))
        .orderBy(desc("session_count"))
    )

    print("\n========== CUSTOMER CONVERSION ==========")
    funnel.show(truncate=False)

    # 7. Monthly revenue trend
    monthly_sales = (
        delivered
        .withColumn("sale_date", to_date(col("timestamp")))
        .withColumn("month", date_format(col("sale_date"), "yyyy-MM"))
        .groupBy("month")
        .agg(
            spark_round(spark_sum("total"), 2).alias("monthly_revenue"),
            countDistinct("transaction_id").alias("transactions")
        )
        .orderBy("month")
    )

    print("\n========== MONTHLY SALES ==========")
    monthly_sales.show(truncate=False)

    # 8. Session and transaction integration by user
    session_summary = (
        sessions.groupBy("user_id")
        .agg(
            count("*").alias("total_sessions"),
            spark_sum(
                (col("conversion_status") == "converted").cast("integer")
            ).alias("converted_sessions")
        )
    )

    customer_journey = (
        session_summary
        .join(customer_value, "user_id", "left")
        .fillna(0, subset=[
            "purchase_count", "lifetime_value",
            "total_sessions", "converted_sessions"
        ])
        .orderBy(desc("lifetime_value"))
    )

    print("\n========== INTEGRATED CUSTOMER JOURNEY ==========")
    customer_journey.show(10, truncate=False)

    # 9. Export integrated analytics
    results = {
        "integrated_customer_value": customer_value,
        "product_affinity": product_affinity,
        "integrated_conversion": funnel,
        "monthly_sales": monthly_sales,
        "customer_journey": customer_journey
    }

    for name, dataframe in results.items():
        output_path = OUTPUT_DIR / name
        dataframe.coalesce(1).write.mode("overwrite").option(
            "header", "true"
        ).csv(
            "file:///" + str(output_path.resolve()).replace("\\", "/")
        )
        print("Exported:", name)

    print("\n========== INTEGRATED ANALYTICS COMPLETED ==========")
    print("Results saved in:", OUTPUT_DIR)

    spark.stop()

if __name__ == "__main__":
    main()
