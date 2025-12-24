# Databricks notebook source
import sys
import os
# Go two levels up to reach the project root
project_root = os.path.abspath(os.path.join(os.getcwd(), "../.."))

if project_root not in sys.path:
    sys.path.append(project_root)

from pyspark.sql.functions import date_format, col, lit
from modules.utils.date_utils import get_month_start_n_months_ago

# COMMAND ----------

two_months_ago_start = get_month_start_n_months_ago(2)
one_month_ago_start  = get_month_start_n_months_ago(1)

# COMMAND ----------

# Read the 'trips_enriched' table from the 'nyctaxi.02_silver' schema
# and filter to only include trips with a pickup datetime
# later than the start date from two months ago

df_yellow = (
    spark.read.table("nyctaxi.02_silver.yellow_trips_enriched")
    .filter((col("pickup_datetime") >= lit(str(two_months_ago_start))) &
            (col("pickup_datetime") <  lit(str(one_month_ago_start))))
)

df_green = (
    spark.read.table("nyctaxi.02_silver.green_trips_enriched")
    .filter((col("pickup_datetime") >= lit(str(two_months_ago_start))) &
            (col("pickup_datetime") <  lit(str(one_month_ago_start))))
)

df = df_yellow.unionByName(df_green)

# COMMAND ----------

# Add a year_month column, formated as yyyy-MM

df = df.withColumn("year_month", date_format("pickup_datetime", "yyyy-MM"))

# COMMAND ----------

df.write \
  .option("path", "abfss://nyctaxi@nyctaxistorage888.dfs.core.windows.net/taxi_trips_export/") \
  .format("json") \
  .mode("append") \
  .partitionBy("taxi_type", "vendor", "year_month") \
  .saveAsTable("nyctaxi.04_export.taxi_trips_export")
