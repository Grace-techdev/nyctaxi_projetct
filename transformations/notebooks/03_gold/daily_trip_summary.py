# Databricks notebook source
import sys
import os
# Go two levels up to reach the project root
project_root = os.path.abspath(os.path.join(os.getcwd(), "../.."))

if project_root not in sys.path:
    sys.path.append(project_root)

from pyspark.sql.functions import count, max, min, avg, sum, round, col
from modules.utils.date_utils import get_month_start_n_months_ago

# COMMAND ----------

# Get the first day of the month two months ago
two_months_ago_start = get_month_start_n_months_ago(2)
one_month_ago_start  = get_month_start_n_months_ago(1)

# COMMAND ----------

# Load the enriched trip dataset
# and filter to only include trips with a pickup datetime later than the start date from two months ago
df_yellow = (
    spark.read.table("nyctaxi.02_silver.yellow_trips_enriched")
    .filter((col("pickup_datetime") >= two_months_ago_start) &
            (col("pickup_datetime") <  one_month_ago_start))
)

df_green = (
    spark.read.table("nyctaxi.02_silver.green_trips_enriched")
    .filter((col("pickup_datetime") >= two_months_ago_start) &
            (col("pickup_datetime") <  one_month_ago_start))
)

df = df_yellow.unionByName(df_green)

# COMMAND ----------

# Aggregate trip data by pickup date with key metrics
df_gold = (
    df
    .groupBy(
        col("taxi_type"),
        col("pickup_datetime").cast("date").alias("pickup_date")
    )
    .agg(
        count("*").alias("total_trips"),
        round(avg("passenger_count"), 1).alias("average_passengers"),
        round(avg("trip_distance"), 1).alias("average_distance"),
        round(avg("fare_amount"), 2).alias("average_fare_per_trip"),
        max("fare_amount").alias("max_fare"),
        min("fare_amount").alias("min_fare"),
        round(sum("total_amount"), 2).alias("total_revenue")
    )
)

# COMMAND ----------

# Write the daily summary to a Unity Catalog managed Delta table in the gold schema
df_gold.write.mode("append").saveAsTable("nyctaxi.03_gold.daily_trip_summary")
