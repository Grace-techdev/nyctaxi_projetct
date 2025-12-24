# Databricks notebook source
# MAGIC %md
# MAGIC In this section, we analyze the enriched NYC taxi trip data across both Yellow and Green taxi services to validate data quality, understand key patterns, and compare operational characteristics between taxi types.

# COMMAND ----------

from pyspark.sql.functions import *

df_yellow = spark.read.table("nyctaxi.02_silver.yellow_trips_enriched")
df_green  = spark.read.table("nyctaxi.02_silver.green_trips_enriched")

df = df_yellow.unionByName(df_green)

# COMMAND ----------

# MAGIC %md
# MAGIC **Which vendor makes the most revenue?**

# COMMAND ----------

df.groupBy("taxi_type", "vendor") \
  .agg(sum("total_amount").alias("total_revenue")) \
  .orderBy(desc("total_revenue")) \
  .display()

# COMMAND ----------

# MAGIC %md
# MAGIC **What is the most popular pickup borough?**

# COMMAND ----------

df.groupBy("taxi_type", "pu_borough") \
  .agg(count("*").alias("number_of_trips")) \
  .orderBy(desc("number_of_trips")) \
  .display()

# COMMAND ----------

# MAGIC %md
# MAGIC **What is the most common journey (borough to borough)?**

# COMMAND ----------

df.groupBy("taxi_type", "pu_borough", "do_borough") \
  .agg(count("*").alias("number_of_trips")) \
  .orderBy(desc("number_of_trips")) \
  .display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Create a time series chart showing the number of trips and total revenue per day**

# COMMAND ----------

df2 = spark.read.table("nyctaxi.03_gold.daily_trip_summary")

# COMMAND ----------

df2.filter(col("taxi_type") == "yellow").orderBy("pickup_date").display()

# COMMAND ----------

df2.filter(col("taxi_type") == "green").orderBy("pickup_date").display()

# COMMAND ----------

# MAGIC %md
# MAGIC
