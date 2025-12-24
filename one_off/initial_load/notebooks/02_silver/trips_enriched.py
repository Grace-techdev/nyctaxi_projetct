# Databricks notebook source
from pyspark.sql.functions import col, lit

# COMMAND ----------

dbutils.widgets.text("taxi_type", "green")
taxi_type = dbutils.widgets.get("taxi_type")

# COMMAND ----------

df_trips = spark.read.table(f"nyctaxi.02_silver.{taxi_type}_trips_cleansed")
df_zones = spark.read.table("nyctaxi.02_silver.taxi_zone_lookup")

# COMMAND ----------

zones_pu = df_zones.alias("zones_pu")

df_join_1 = df_trips.join(
    zones_pu,
    df_trips.pu_location_id == zones_pu.location_id,
    "left",
).select(
    df_trips.vendor,
    df_trips.pickup_datetime,
    df_trips.dropoff_datetime,
    df_trips.trip_duration,
    df_trips.passenger_count,
    df_trips.trip_distance,
    df_trips.rate_type,
    col("zones_pu.borough").alias("pu_borough"),
    col("zones_pu.zone").alias("pu_zone"),
    df_trips.do_location_id,
    df_trips.payment_type,
    df_trips.fare_amount,
    df_trips.extra,
    df_trips.mta_tax,
    df_trips.tolls_amount,
    df_trips.improvement_surcharge,
    df_trips.total_amount,
    df_trips.congestion_surcharge,
    df_trips.airport_fee,
    df_trips.trip_type,
    df_trips.cbd_congestion_fee,
    df_trips.processed_timestamp,
    df_trips.taxi_type
)

# COMMAND ----------

zones_do = df_zones.alias("zones_do")

df_join_final = df_join_1.join(
    zones_do,
    df_join_1.do_location_id == zones_do.location_id,
    "left",
).select(
    df_join_1.vendor,
    df_join_1.pickup_datetime,
    df_join_1.dropoff_datetime,
    df_join_1.trip_duration,
    df_join_1.passenger_count,
    df_join_1.trip_distance,
    df_join_1.rate_type,
    df_join_1.pu_borough,
    col("zones_do.borough").alias("do_borough"),
    df_join_1.pu_zone,
    col("zones_do.zone").alias("do_zone"),
    df_join_1.payment_type,
    df_join_1.fare_amount,
    df_join_1.extra,
    df_join_1.mta_tax,
    df_join_1.tolls_amount,
    df_join_1.improvement_surcharge,
    df_join_1.total_amount,
    df_join_1.congestion_surcharge,
    df_join_1.airport_fee,
    df_join_1.trip_type,
    df_join_1.cbd_congestion_fee,
    df_join_1.processed_timestamp,
    df_join_1.taxi_type
)

# COMMAND ----------

df_join_final.write.mode("overwrite").saveAsTable(f"nyctaxi.02_silver.{taxi_type}_trips_enriched")
