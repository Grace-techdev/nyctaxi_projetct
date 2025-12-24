# Databricks notebook source
import sys
import os
# Go two levels up to reach the project root
project_root = os.path.abspath(os.path.join(os.getcwd(), "../.."))

if project_root not in sys.path:
    sys.path.append(project_root)

from pyspark.sql.functions import col, when, timestamp_diff, lit
from modules.utils.date_utils import get_month_start_n_months_ago

# COMMAND ----------

dbutils.widgets.text("taxi_type", "green")
taxi_type = dbutils.widgets.get("taxi_type")

# COMMAND ----------

# Get the first day of the month two months ago
two_months_ago_start = get_month_start_n_months_ago(2)

# Get the first day of the month one month ago
one_month_ago_start = get_month_start_n_months_ago(1)

# COMMAND ----------

if taxi_type == "yellow":
    pickup_col = "tpep_pickup_datetime"
    dropoff_col = "tpep_dropoff_datetime"
    airport_fee_expr = col("Airport_fee")
    trip_type_expr = lit(None).cast("int")
elif taxi_type == "green":
    pickup_col = "lpep_pickup_datetime"
    dropoff_col = "lpep_dropoff_datetime"
    airport_fee_expr = lit(None).cast("double")
    trip_type_expr = col("trip_type").cast("int")

# COMMAND ----------

# Read the 'yellow_trips_raw' table from the 'nyctaxi.01_bronze' schema
# Then filter rows where 'tpep_pickup_datetime' is >= two months ago start
# and < one month ago start (i.e., only the month that is two months before today)

df = (
    spark.read.table(f"nyctaxi.01_bronze.{taxi_type}_trips_raw")
    .filter((col(pickup_col) >= lit(str(two_months_ago_start))) & (col(pickup_col) < lit(str(one_month_ago_start))))
)

# COMMAND ----------

# Select and transform fields, decoding codes and computing duration
df = df.select(
    when(col("VendorID") == 1, "Creative Mobile Technologies, LLC")
      .when(col("VendorID") == 2, "Curb Mobility, LLC")
      .when(col("VendorID") == 6, "Myle Technologies Inc")
      .when(col("VendorID") == 7, "Helix")
      .otherwise("Unknown")
      .alias("vendor"),

    col(pickup_col).alias("pickup_datetime"),
    col(dropoff_col).alias("dropoff_datetime"),
    timestamp_diff("MINUTE", col(pickup_col), col(dropoff_col)).alias("trip_duration"),

    "passenger_count",
    "trip_distance",

    when(col("RatecodeID") == 1, "Standard Rate")
      .when(col("RatecodeID") == 2, "JFK")
      .when(col("RatecodeID") == 3, "Newark")
      .when(col("RatecodeID") == 4, "Nassau or Westchester")
      .when(col("RatecodeID") == 5, "Negotiated Fare")
      .when(col("RatecodeID") == 6, "Group Ride")
      .otherwise("Unknown")
      .alias("rate_type"),

    "store_and_fwd_flag",
    col("PULocationID").alias("pu_location_id"),
    col("DOLocationID").alias("do_location_id"),

    when(col("payment_type") == 0, "Flex Fare trip")
      .when(col("payment_type") == 1, "Credit card")
      .when(col("payment_type") == 2, "Cash")
      .when(col("payment_type") == 3, "No charge")
      .when(col("payment_type") == 4, "Dispute")
      .when(col("payment_type") == 6, "Voided trip")
      .otherwise("Unknown")
      .alias("payment_type"),

    "fare_amount",
    "extra",
    "mta_tax",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
    airport_fee_expr.alias("airport_fee"),
    trip_type_expr.alias("trip_type"),
    "cbd_congestion_fee",
    "processed_timestamp",
    lit(taxi_type).alias("taxi_type")
)

# COMMAND ----------

# Write cleansed data to a Unity Catalog managed Delta table in the silver schema
df.write.mode("append").saveAsTable(f"nyctaxi.02_silver.{taxi_type}_trips_cleansed")
