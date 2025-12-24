# Databricks notebook source
from pyspark.sql.functions import date_format

# COMMAND ----------

# Add a year_month column, formated as yyyy-MM
df_yellow = spark.read.table("nyctaxi.02_silver.yellow_trips_enriched")
df_green  = spark.read.table("nyctaxi.02_silver.green_trips_enriched")
df = df_yellow.unionByName(df_green)

df = df.withColumn("year_month", date_format("pickup_datetime", "yyyy-MM"))

# COMMAND ----------

df.write \
  .option("path", "abfss://nyctaxi@nyctaxistorage888.dfs.core.windows.net/taxi_trips_export/") \
  .format("json") \
  .mode("append") \
  .partitionBy("taxi_type", "vendor", "year_month") \
  .saveAsTable("nyctaxi.04_export.taxi_trips_export")
