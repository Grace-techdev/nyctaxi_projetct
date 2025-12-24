# Databricks notebook source
from pyspark.sql.functions import col, count, max, min, avg, sum, round

# COMMAND ----------

df_yellow = spark.read.table("nyctaxi.02_silver.yellow_trips_enriched")
df_green  = spark.read.table("nyctaxi.02_silver.green_trips_enriched")
df = df_yellow.unionByName(df_green)

# COMMAND ----------

df_gold = (
    df.\
        groupBy(
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

df_gold.write.mode("overwrite").saveAsTable("nyctaxi.03_gold.daily_trip_summary")
