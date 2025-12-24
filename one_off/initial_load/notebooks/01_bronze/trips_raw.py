# Databricks notebook source
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

dbutils.widgets.text("taxi_type", "green")
taxi_type = dbutils.widgets.get("taxi_type")

# COMMAND ----------

df = spark.read.format("parquet").load(
    f"/Volumes/nyctaxi/00_landing/data_sources/nyctaxi_{taxi_type}/*/*.parquet"
)

# COMMAND ----------

df = df.withColumn("processed_timestamp",current_timestamp())

# COMMAND ----------

df.write.mode("append").saveAsTable(f"nyctaxi.01_bronze.{taxi_type}_trips_raw") 
