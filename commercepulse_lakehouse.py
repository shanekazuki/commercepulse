# Databricks notebook source
# MAGIC %md
# MAGIC # CommercePulse — Bronze / Silver / Gold ELT
# MAGIC Upload the sample CSV to a Unity Catalog volume or DBFS path, then set the
# MAGIC source_path, catalog, and schema widgets. This notebook requires a running
# MAGIC Databricks cluster and permission to create Delta tables in the target schema.

# COMMAND ----------

dbutils.widgets.text("source_path", "dbfs:/FileStore/commercepulse/orders.csv")
dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "commercepulse")

source_path = dbutils.widgets.get("source_path")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

from pyspark.sql import functions as F

# Accept only simple identifiers before using names in SQL statements.
import re

for identifier in (catalog, schema):
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", identifier):
        raise ValueError(f"Invalid catalog/schema identifier: {identifier!r}")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
spark.sql(f"USE CATALOG `{catalog}`")
spark.sql(f"USE SCHEMA `{schema}`")

# COMMAND ----------

# Bronze: preserve source values as strings and retain ingestion metadata.
raw = (
    spark.read.option("header", True)
    .option("inferSchema", False)
    .csv(source_path)
    .withColumn("_source_file", F.input_file_name())
    .withColumn("_ingested_at", F.current_timestamp())
)

# COMMAND ----------

from pyspark.sql.window import Window

raw.write.format("delta").mode("overwrite").saveAsTable("bronze_orders")
print(f"Bronze rows: {raw.count()}")

# COMMAND ----------

# Silver: cast/standardize, validate business fields, and keep the latest row
# for each source order_item_id. Invalid records remain available in Bronze.
bronze = spark.table("bronze_orders")
typed = (
    bronze
    .withColumn("quantity", F.col("quantity").cast("int"))
    .withColumn("unit_price", F.col("unit_price").cast("decimal(12,2)"))
    .withColumn("discount_amount", F.col("discount_amount").cast("decimal(12,2)"))
    .withColumn("order_date", F.to_date("order_date"))
    .withColumn("promised_delivery_date", F.to_date("promised_delivery_date"))
    .withColumn("delivered_date", F.to_date("delivered_date"))
    .withColumn("order_item_id", F.trim("order_item_id"))
    .withColumn("order_id", F.trim("order_id"))
    .withColumn("customer_id", F.trim("customer_id"))
    .withColumn("product_id", F.trim("product_id"))
    .withColumn("revenue", F.col("quantity") * F.col("unit_price") - F.col("discount_amount"))
    .withColumn(
        "delivery_delay_days",
        F.datediff("delivered_date", "promised_delivery_date"),
    )
    .withColumn(
        "is_on_time",
        F.when(
            F.col("delivered_date").isNotNull()
            & F.col("promised_delivery_date").isNotNull(),
            F.col("delivery_delay_days") <= 0,
        ),
    )
)
valid = typed.filter(
    F.col("order_item_id").isNotNull()
    & (F.col("order_item_id") != "")
    & F.col("order_id").isNotNull()
    & F.col("customer_id").isNotNull()
    & F.col("product_id").isNotNull()
    & F.col("order_date").isNotNull()
    & (F.col("quantity") > 0)
    & (F.col("unit_price") >= 0)
    & (F.col("discount_amount") >= 0)
    & (F.col("discount_amount") <= F.col("quantity") * F.col("unit_price"))
)
dedupe_window = Window.partitionBy("order_item_id").orderBy(F.col("_ingested_at").desc())
silver = (
    valid.withColumn("_row_number", F.row_number().over(dedupe_window))
    .filter(F.col("_row_number") == 1)
    .drop("_row_number")
)
silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    "silver_orders"
)
print(f"Silver rows: {silver.count()}; rejected rows: {typed.count() - valid.count()}")

# COMMAND ----------

# Gold marts: equivalent definitions are also supplied as portable SQL in sql/analytics.sql.
silver.createOrReplaceTempView("silver_orders_view")

spark.sql("""
CREATE OR REPLACE TABLE gold_daily_sales USING DELTA AS
SELECT order_date, ROUND(SUM(revenue), 2) AS net_revenue,
       COUNT(DISTINCT order_id) AS order_count,
       ROUND(SUM(revenue) / COUNT(DISTINCT order_id), 2) AS average_order_value,
       SUM(quantity) AS units_sold
FROM silver_orders_view
GROUP BY order_date
""")

spark.sql("""
CREATE OR REPLACE TABLE gold_product_performance USING DELTA AS
SELECT product_id, MAX(product_name) AS product_name, MAX(category) AS category,
       ROUND(SUM(revenue), 2) AS net_revenue, SUM(quantity) AS units_sold,
       COUNT(DISTINCT order_id) AS order_count
FROM silver_orders_view
GROUP BY product_id
""")

spark.sql("""
CREATE OR REPLACE TABLE gold_customer_repeat_purchase USING DELTA AS
SELECT customer_id, COUNT(DISTINCT order_id) AS order_count,
       COUNT(DISTINCT order_id) > 1 AS is_repeat_customer,
       ROUND(SUM(revenue), 2) AS lifetime_revenue
FROM silver_orders_view
GROUP BY customer_id
""")

spark.sql("""
CREATE OR REPLACE TABLE gold_delivery_performance USING DELTA AS
SELECT order_date, COUNT(*) AS delivered_order_items,
       SUM(CASE WHEN is_on_time THEN 1 ELSE 0 END) AS on_time_order_items,
       SUM(CASE WHEN NOT is_on_time THEN 1 ELSE 0 END) AS late_order_items,
       ROUND(100.0 * SUM(CASE WHEN is_on_time THEN 1 ELSE 0 END) / COUNT(*), 2)
           AS on_time_rate_pct,
       ROUND(AVG(delivery_delay_days), 2) AS average_delay_days
FROM silver_orders_view
WHERE delivered_date IS NOT NULL AND promised_delivery_date IS NOT NULL
GROUP BY order_date
""")

# COMMAND ----------

display(spark.table("gold_daily_sales").orderBy("order_date"))
display(spark.table("gold_product_performance").orderBy(F.desc("net_revenue")))
display(spark.table("gold_customer_repeat_purchase").orderBy(F.desc("order_count")))
display(spark.table("gold_delivery_performance").orderBy("order_date"))
