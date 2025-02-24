# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import * 

# COMMAND ----------

JAAS_MODULE = "org.apache.kafka.common.security.plain.PlainLoginModule"
USERNAME = ''
PASSWORD = ''

# COMMAND ----------

kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "pkc-921jm.us-east-2.aws.confluent.cloud:9092")
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.mechanism", "PLAIN")
    .option("kafka.sasl.jaas.config", f"{JAAS_MODULE} required username='{USERNAME}' password='{PASSWORD}';")
    .option("subscribe", "ride-request")
    .option("startingOffsets", "earliest")
    .load()
)

# COMMAND ----------

kafka_df.printSchema()

# COMMAND ----------

# DBTITLE 1,Deserialize JSON
kafka_df = kafka_df.withColumn("key",expr("cast(key as string)")).withColumn("value",expr("cast(value as string)"))

# COMMAND ----------

ride_schema = StructType([
    StructField("ride_id",StringType()),
    StructField("user_id",StringType()),
    StructField("driver_id",StringType()),
    StructField("pickup_location",StringType()),
    StructField("drop_location",StringType()),
    StructField("fare",FloatType()),
    StructField("distance_km",FloatType()),
    StructField("request_time",StringType()),
    StructField("pickup_time",StringType()),
    StructField("drop_time",StringType()),
])

# COMMAND ----------

parse_df = kafka_df.select(from_json(col("value"),ride_schema).alias("data")).select("data.*")


# COMMAND ----------

parse_df.printSchema()

# COMMAND ----------

df = (
    parse_df.withColumn("request_time", to_timestamp("request_time", "yyyy-MM-dd HH:mm:ss")) 
    .withColumn("pickup_time", to_timestamp("pickup_time", "yyyy-MM-dd HH:mm:ss"))
    .withColumn("drop_time", to_timestamp("drop_time", "yyyy-MM-dd HH:mm:ss"))
)

# COMMAND ----------

df = df.withColumn("estimated_arrival_time", expr("distance_km * 2"))
df = df.withColumn("surge_multiplier", expr("CASE WHEN distance_km > 20 THEN 1.5 ELSE 1.0 END"))

# COMMAND ----------

(
    df.writeStream 
    .format("delta") 
    .outputMode("append") 
    .option("checkpointLocation", "/FileStore/checkpoints/ride_data")   
    .option("path", "/FileStore/delta/ride_data")  
    .table("ride_data")
)


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ride_data

# COMMAND ----------

# MAGIC %md
# MAGIC # Basic Analysis

# COMMAND ----------

# DBTITLE 1,Total Rides Per Hour
# MAGIC %sql
# MAGIC SELECT 
# MAGIC     HOUR(request_time) AS hour_of_day, 
# MAGIC     COUNT(*) AS total_rides
# MAGIC FROM ride_data
# MAGIC GROUP BY hour_of_day
# MAGIC ORDER BY hour_of_day;
# MAGIC

# COMMAND ----------

# DBTITLE 1,Average Fare per Kilometer
# MAGIC %sql
# MAGIC SELECT 
# MAGIC     AVG(fare / distance_km) AS avg_fare_per_km
# MAGIC FROM ride_data
# MAGIC WHERE distance_km > 0;
# MAGIC

# COMMAND ----------

# DBTITLE 1,Top 5 Busiest Pickup Locations
# MAGIC %sql
# MAGIC SELECT 
# MAGIC     pickup_location, 
# MAGIC     COUNT(*) AS total_pickups
# MAGIC FROM ride_data
# MAGIC GROUP BY pickup_location
# MAGIC ORDER BY total_pickups DESC
# MAGIC LIMIT 5;
# MAGIC

# COMMAND ----------

# DBTITLE 1,Surge Pricing Analysis
# MAGIC %sql
# MAGIC SELECT 
# MAGIC     request_time, 
# MAGIC     pickup_location, 
# MAGIC     drop_location, 
# MAGIC     fare, 
# MAGIC     distance_km, 
# MAGIC     (fare / distance_km) AS fare_per_km
# MAGIC FROM ride_data
# MAGIC WHERE (fare / distance_km) > (SELECT AVG(fare / distance_km) * 1.5 FROM ride_data)
# MAGIC ORDER BY request_time DESC;
# MAGIC

# COMMAND ----------

# DBTITLE 1,Ride Duration & Traffic Patterns
# MAGIC %sql
# MAGIC SELECT 
# MAGIC     CASE 
# MAGIC         WHEN distance_km < 5 THEN 'Short (<5 km)'
# MAGIC         WHEN distance_km BETWEEN 5 AND 15 THEN 'Medium (5-15 km)'
# MAGIC         ELSE 'Long (>15 km)'
# MAGIC     END AS ride_category,
# MAGIC     AVG(UNIX_TIMESTAMP(drop_time) - UNIX_TIMESTAMP(pickup_time)) / 60 AS avg_duration_minutes
# MAGIC FROM ride_data
# MAGIC GROUP BY ride_category;
# MAGIC

# COMMAND ----------


