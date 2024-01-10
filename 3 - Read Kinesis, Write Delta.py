# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS hosea;
# MAGIC CREATE SCHEMA IF NOT EXISTS hosea.kinesis;

# COMMAND ----------

# loading the data from Kinesis Stream1
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime

# replace with your stream names
STREAM_NAME1 = 
STREAM_NAME2 = 
REGION_NAME = 
aws_access_key_id = 
aws_secret_key = 

# COMMAND ----------

kinesisDF1 = spark \
  .readStream \
  .format("kinesis") \
  .option("streamName", STREAM_NAME1) \
  .option("initialPosition", "TRIM_HORIZON") \
  .option("region", REGION_NAME) \
  .option("awsAccessKey", aws_access_key_id) \
  .option("awsSecretKey", aws_secret_key) \
  .load()


# display(newdf1)

# COMMAND ----------

# Deserializing the data from JSON back to a DF for Stream1

my_udf = udf(lambda x: x.decode(),StringType())
newdf1 = kinesisDF1.withColumn('jsonString', my_udf('data'))

schema = StructType([
    StructField("Ride_start_time_stamp", TimestampType(), True),
    StructField("RideID", IntegerType(), True),
    StructField("Ride_length_estimate_mins", IntegerType(), True),
    StructField("City", StringType(), True),
])

options = {'mode': 'FAILFAST'}
cleanDF1 = newdf1\
    .selectExpr('CAST(jsonString AS STRING)')\
    .select(from_json('jsonString', schema, options).alias('cleandata'))\
    .select('cleandata.*')\

# display(cleanDF1)

# COMMAND ----------

# recuresively deletes checkpoint so stream can be restarted from beginning without failure 

dbutils.fs.rm("checkpointpath1", True)
dbutils.fs.rm("checkpointpath2", True)

# COMMAND ----------

# write to delta stream1

(cleanDF1.writeStream\
  .format("delta")\
  .outputMode("append")\
  .option("checkpointLocation", "checkpointpath1")\
  .toTable('hosea.kinesis.delta_stream1')\
)

# COMMAND ----------



# COMMAND ----------

# loading the data from Kinesis Stream2


kinesisDF2 = spark \
  .readStream \
  .format("kinesis") \
  .option("streamName", STREAM_NAME2) \
  .option("initialPosition", "TRIM_HORIZON") \
  .option("region", REGION_NAME) \
  .option("awsAccessKey", aws_access_key_id) \
  .option("awsSecretKey", aws_secret_key) \
  .load()

my_udf2 = udf(lambda x: x.decode(),StringType())
newdf2 = kinesisDF2.withColumn('jsonString', my_udf2('data'))


# display(newdf2)

# COMMAND ----------

# Deserializing the data from JSON back to a DF for Stream2

schema2 = StructType([
    StructField("Ride_end_time_stamp", TimestampType(), True),
    StructField("RideID", IntegerType(), True)])

options = {'mode': 'FAILFAST'} # FAILFAST # PERMISSIVE
cleanDF2 = newdf2\
    .selectExpr('CAST(jsonString AS STRING)')\
    .select(from_json('jsonString', schema2).alias('cleandata2'))\
    .select('cleandata2.*')\

# display(cleanDF2)

# COMMAND ----------

# write to delta stream2

(cleanDF2.writeStream\
  .format("delta")\
  .outputMode("append")\
  .option("checkpointLocation", "checkpointpath2")\
  .toTable('hosea.kinesis.delta_stream2')\
)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- must use DBR 13+ cluster, watermark feature uses Spark 3.0
# MAGIC SELECT *
# MAGIC FROM hosea.kinesis.delta_stream1
# MAGIC WATERMARK Ride_start_time_stamp 
# MAGIC DELAY OF INTERVAL 10 SECONDS

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM hosea.kinesis.delta_stream2 
# MAGIC WATERMARK Ride_end_time_stamp
# MAGIC DELAY OF INTERVAL 10 SECONDS;
