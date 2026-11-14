
import os
import sys
from pathlib import Path

# ==================================================
# 1. SPARK AND PYTHON CONFIGURATION
# ==================================================

SPARK_HOME = (
    r"C:\Users\i7\Downloads\spark-3.5.9-bin-hadoop3"
    r"\spark-3.5.9-bin-hadoop3"
)

os.environ["SPARK_HOME"] = SPARK_HOME
os.environ["PYSPARK_PYTHON"] = "python"
os.environ["PYSPARK_DRIVER_PYTHON"] = "python"

spark_lib = Path(SPARK_HOME) / "python" / "lib"

sys.path.extend([
    str(spark_lib / "pyspark.zip"),
    str(spark_lib / "py4j-0.10.9.7-src.zip")
])

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    trim,
    to_timestamp,
    to_date,
    sum,
    count,
    avg,
    explode,
    coalesce,
    round,
    desc
)

# ==================================================
# 2. PROJECT DIRECTORIES
# ==================================================

PROJECT_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_DIR / "data"
OUTPUT_DIR = PROJECT_DIR / "outputs"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

def local_file(filename):
    path = (DATA_DIR / filename).resolve()
    return "file:///" + str(path).replace("\\", "/")

# ==================================================
# 3. START SPARK
# ==================================================

spark = (
    SparkSession.builder
    .appName("EcommerceDistributedAnalytics")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.hadoop.fs.defaultFS", "file:///")
    .getOrCreate()
)

# Explicitly use the local Windows filesystem
spark.sparkContext._jsc.hadoopConfiguration().set(
    "fs.defaultFS", "file:///"
)

spark.sparkContext.setLogLevel("WARN")

print("\n===================================")
print("DISTRIBUTED E-COMMERCE ANALYTICS")
print("===================================")
print("Spark Version:", spark.version)

# ==================================================
# 4. LOAD JSON DATASETS
# ==================================================

print("\nSTEP 1: LOADING DATASETS")

users = spark.read.option("multiLine", "true").json(
    local_file("users.json")
)

products = spark.read.option("multiLine", "true").json(
    local_file("products.json")
)

categories = spark.read.option("multiLine", "true").json(
    local_file("categories.json")
)

transactions = spark.read.option("multiLine", "true").json(
    local_file("transactions.json")
)

sessions = spark.read.option("multiLine", "true").json(
    local_file("sessions_0.json")
)

print("Users:", users.count())
print("Products:", products.count())
print("Categories:", categories.count())
print("Transactions:", transactions.count())
print("Sessions:", sessions.count())

# ==================================================
# 5. DATA CLEANING AND NORMALIZATION
# ==================================================

print("\nSTEP 2: CLEANING DATA")

users = (
    users
    .withColumn("user_id", trim(col("user_id")))
    .filter(col("user_id").isNotNull())
    .dropDuplicates(["user_id"])
)

products = (
    products
    .withColumn("product_id", trim(col("product_id")))
    .withColumn("base_price", col("base_price").cast("double"))
    .filter(
        col("product_id").isNotNull()
        & (col("base_price") >= 0)
    )
    .dropDuplicates(["product_id"])
)

categories = (
    categories
    .withColumn("category_id", trim(col("category_id")))
    .filter(col("category_id").isNotNull())
    .dropDuplicates(["category_id"])
)

transactions = (
    transactions
    .withColumn("transaction_id", trim(col("transaction_id")))
    .withColumn("user_id", trim(col("user_id")))
    .withColumn("total", col("total").cast("double"))
    .withColumn("timestamp", to_timestamp(col("timestamp")))
    .filter(
        col("transaction_id").isNotNull()
        & col("user_id").isNotNull()
        & col("total").isNotNull()
        & (col("total") >= 0)
        & col("timestamp").isNotNull()
    )
    .dropDuplicates(["transaction_id"])
)

sessions = (
    sessions
    .withColumn("session_id", trim(col("session_id")))
    .withColumn("user_id", trim(col("user_id")))
    .withColumn("start_time", to_timestamp(col("start_time")))
    .filter(
        col("session_id").isNotNull()
        & col("user_id").isNotNull()
    )
    .dropDuplicates(["session_id"])
)

print("Clean users:", users.count())
print("Clean products:", products.count())
print("Clean categories:", categories.count())
print("Clean transactions:", transactions.count())
print("Clean sessions:", sessions.count())

# ==================================================
# 6. PREPARE TRANSACTION ITEMS
# ==================================================

print("\nSTEP 3: PREPARING TRANSACTION ITEMS")

delivered = transactions.filter(
    col("status") == "delivered"
)

items = (
    delivered
    .select(
        "transaction_id",
        "user_id",
        "timestamp",
        explode("items").alias("item")
    )
    .select(
        "transaction_id",
        "user_id",
        "timestamp",
        col("item.product_id").alias("product_id"),
        col("item.quantity").cast("double").alias("quantity"),
        col("item.unit_price").cast("double").alias("unit_price"),
        col("item.subtotal").cast("double").alias("item_subtotal")
    )
    .filter(
        col("product_id").isNotNull()
        & col("quantity").isNotNull()
    )
)

items = items.withColumn(
    "item_revenue",
    coalesce(
        col("item_subtotal"),
        col("quantity") * col("unit_price")
    )
)

# Register data for Spark SQL
delivered.createOrReplaceTempView("transactions")
items.createOrReplaceTempView("transaction_items")
products.createOrReplaceTempView("products")
categories.createOrReplaceTempView("categories")
users.createOrReplaceTempView("users")
sessions.createOrReplaceTempView("sessions")

# ==================================================
# 7. SPARK SQL ANALYTICS
# ==================================================

print("\nSTEP 4: RUNNING SPARK SQL")

# Analysis 1: Sales performance by category

sales_by_category = spark.sql("""
    SELECT
        c.name AS category,
        ROUND(SUM(i.item_revenue), 2) AS revenue,
        SUM(i.quantity) AS units_sold,
        COUNT(DISTINCT i.transaction_id) AS transactions
    FROM transaction_items i
    JOIN products p
        ON i.product_id = p.product_id
    JOIN categories c
        ON p.category_id = c.category_id
    GROUP BY c.name
    ORDER BY revenue DESC
""")

# Analysis 2: Customer purchase value

customer_value = spark.sql("""
    SELECT
        user_id,
        COUNT(transaction_id) AS purchase_count,
        ROUND(SUM(total), 2) AS total_spent,
        ROUND(AVG(total), 2) AS average_order_value
    FROM transactions
    GROUP BY user_id
    ORDER BY total_spent DESC
""")

# ==================================================
# 8. PRODUCT PERFORMANCE
# ==================================================

print("\nSTEP 5: PRODUCT PERFORMANCE")

product_performance = (
    items
    .join(
        products.select("product_id", "name", "category_id"),
        on="product_id",
        how="left"
    )
    .groupBy("product_id", "name", "category_id")
    .agg(
        round(sum("item_revenue"), 2).alias("revenue"),
        sum("quantity").alias("units_sold")
    )
    .orderBy(desc("revenue"))
)

# ==================================================
# 9. DAILY SALES TRENDS
# ==================================================

print("\nSTEP 6: DAILY SALES TRENDS")

daily_sales = (
    delivered
    .withColumn("sale_date", to_date("timestamp"))
    .groupBy("sale_date")
    .agg(
        round(sum("total"), 2).alias("daily_revenue"),
        count("transaction_id").alias("transactions")
    )
    .orderBy("sale_date")
)

# ==================================================
# 10. CUSTOMER BROWSING AND CONVERSION FUNNEL
# ==================================================

print("\nSTEP 7: SESSION CONVERSION ANALYSIS")

session_funnel = (
    sessions
    .groupBy("conversion_status")
    .agg(
        count("session_id").alias("session_count")
    )
    .orderBy(desc("session_count"))
)

# ==================================================
# 11. DISPLAY RESULTS
# ==================================================

print("\n========== SALES BY CATEGORY ==========")
sales_by_category.show(truncate=False)

print("\n========== CUSTOMER VALUE ==========")
customer_value.show(10, truncate=False)

print("\n========== PRODUCT PERFORMANCE ==========")
product_performance.show(10, truncate=False)

print("\n========== DAILY SALES ==========")
daily_sales.show(truncate=False)

print("\n========== SESSION CONVERSION FUNNEL ==========")
session_funnel.show(truncate=False)

# ==================================================
# 12. EXPORT RESULTS AS CSV
# ==================================================

print("\nSTEP 8: EXPORTING ANALYTICS RESULTS")

def export_csv(dataframe, folder_name):
    output_path = OUTPUT_DIR / folder_name

    (
        dataframe.coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv("file:///" + str(output_path.resolve()).replace("\\", "/"))
    )

    print("Successfully exported:", folder_name)

export_csv(sales_by_category, "sales_by_category")
export_csv(customer_value, "customer_value")
export_csv(product_performance, "product_performance")
export_csv(daily_sales, "daily_sales")
export_csv(session_funnel, "session_funnel")

# ==================================================
# 13. FINISH
# ==================================================

print("\n===================================")
print("SPARK PROCESSING COMPLETED")
print("===================================")
print("All analytics results saved in:")
print(OUTPUT_DIR)

spark.stop()
