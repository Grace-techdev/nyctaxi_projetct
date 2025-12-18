# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC **Which vendor makes the most revenue?**

# COMMAND ----------

df = spark.read.table("nyctaxi.02_silver.yellow_trips_enriched")

df.display()

# COMMAND ----------

df.\
    groupBy("vendor").\
    agg(
        sum("total_amount").alias("total_revenue")
        ).\
    orderBy("total_revenue", ascending=False).\
    display()

# COMMAND ----------

# MAGIC %md
# MAGIC **What is the most popular pickup borough?**

# COMMAND ----------

df.\
    groupBy("pu_borough").\
    agg(
        count("*").alias("number_of_trips")
    ).\
    orderBy("number_of_trips", ascending=False).\
    display()

# COMMAND ----------

# MAGIC %md
# MAGIC **What is the most common journey (borough to borough)?**

# COMMAND ----------

df.\
    groupBy(concat("pu_borough", lit(" -> "), "do_borough")).\
    agg(
        count("*").alias("number_of_trips")
    ).\
    orderBy("number_of_trips", ascending=False).\
    display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Create a time series chart showing the number of trips and total revenue per day**

# COMMAND ----------

df2 = spark.read.table("nyctaxi.03_gold.daily_trip_summary")

df2.display()

# COMMAND ----------

# MAGIC %md
# MAGIC