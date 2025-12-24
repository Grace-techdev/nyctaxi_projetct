# Databricks notebook source
import sys
import os
# Go two levels up to reach the project root
project_root = os.path.abspath(os.path.join(os.getcwd(), "../.."))

if project_root not in sys.path:
    sys.path.append(project_root)

from modules.utils.date_utils import get_target_yyyymm
from modules.transformations.metadata import add_processed_timestamp
from pyspark.sql.functions import lit

# COMMAND ----------

dbutils.widgets.text("taxi_type", "green")
taxi_type = dbutils.widgets.get("taxi_type")

# COMMAND ----------

# Obtains the year-month for 2 months prior to the current month in yyyy-MM format
formatted_date = get_target_yyyymm(2)

# Read all Parquet files for the specified month from the landing directory into a DataFrame
landing_path = f"/Volumes/nyctaxi/00_landing/data_sources/nyctaxi_{taxi_type}/{formatted_date}"
df = spark.read.format("parquet").load(landing_path)

# COMMAND ----------

# Add a column to capture when the data was processed
df = add_processed_timestamp(df)

# COMMAND ----------

# Write the DataFrame to a Unity Catalog managed Delta table in the bronze schema, appending the new data
df.write.mode("append").saveAsTable(f"nyctaxi.01_bronze.{taxi_type}_trips_raw")
