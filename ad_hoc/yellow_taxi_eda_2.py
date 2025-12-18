# Databricks notebook source
from pyspark.sql.functions import date_format, count, sum

# COMMAND ----------

spark.read.table("nyctaxi.`01_bronze`.yellow_trips_raw").\
    groupBy(date_format("tpep_pickup_datetime", "yyyy-MM").alias("year_month")).\
    agg(count("*").alias("total_records")).\
    orderBy("year_month").display()

# COMMAND ----------

spark.read.table("nyctaxi.`02_silver`.taxi_zone_lookup").display()