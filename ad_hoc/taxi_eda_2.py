# Databricks notebook source
from pyspark.sql.functions import date_format, count, sum

# COMMAND ----------

spark.read.table("nyctaxi.01_bronze.yellow_trips_raw") \
    .groupBy(date_format("tpep_pickup_datetime", "yyyy-MM").alias("year_month")) \
    .agg(count("*").alias("total_records")) \
    .orderBy("year_month") \
    .display()

spark.read.table("nyctaxi.01_bronze.green_trips_raw") \
    .groupBy(date_format("lpep_pickup_datetime", "yyyy-MM").alias("year_month")) \
    .agg(count("*").alias("total_records")) \
    .orderBy("year_month") \
    .display()

# COMMAND ----------

spark.catalog.refreshTable("nyctaxi.02_silver.yellow_trips_enriched")
spark.catalog.refreshTable("nyctaxi.02_silver.green_trips_enriched")

df_yellow = spark.read.table("nyctaxi.02_silver.yellow_trips_enriched")
df_green  = spark.read.table("nyctaxi.02_silver.green_trips_enriched")
print(len(df_yellow.columns), len(df_green.columns))

# COMMAND ----------

df = df_yellow.unionByName(df_green)

# COMMAND ----------

df.groupBy(date_format("pickup_datetime", "yyyy-MM").alias("year_month")) \
  .agg(count("*").alias("total_records")) \
  .orderBy("year_month") \
  .display()

# COMMAND ----------

spark.read.table("nyctaxi.`02_silver`.yellow_trips_enriched").\
    groupBy(date_format("tpep_pickup_datetime", "yyyy-MM").alias("year_month")).\
    agg(count("*").alias("total_records")).\
    orderBy("year_month").display()

# COMMAND ----------

spark.read.table("nyctaxi.03_gold.daily_trip_summary") \
    .groupBy(date_format("pickup_date", "yyyy-MM").alias("year_month")) \
    .agg(count("*").alias("total_records")) \
    .orderBy("year_month") \
    .display()

# COMMAND ----------

spark.read.table("nyctaxi.04_export.taxi_trips_export") \
    .groupBy("year_month") \
    .agg(count("*").alias("total_records")) \
    .orderBy("year_month") \
    .display()

# COMMAND ----------

# MAGIC %sql
# MAGIC REFRESH TABLE nyctaxi.04_export.yellow_trips_export;
