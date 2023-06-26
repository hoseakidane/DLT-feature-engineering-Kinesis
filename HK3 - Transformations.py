# Databricks notebook source
# loading the data from Kinesis Stream1
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime

STREAM_NAME = 
REGION_NAME = 
aws_access_key_id = 
aws_secret_key = 


# COMMAND ----------

kinesisDF1 = spark \
  .readStream \
  .format("kinesis") \
  .option("streamName", STREAM_NAME) \
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
    StructField("RideID", StringType(), True),
    StructField("Ride_length_estimate_mins", StringType(), True),
    StructField("City", StringType(), True),
])

options = {'mode': 'FAILFAST'}
cleanDF1 = newdf1\
    .selectExpr('CAST(jsonString AS STRING)')\
    .select(from_json('jsonString', schema, options).alias('cleandata'))\
    .select('cleandata.*')\

# display(cleanDF1)

# COMMAND ----------

# loading the data from Kinesis Stream2

STREAM_NAME2 = 
REGION_NAME = 
aws_access_key_id = 
aws_secret_key = 

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
    StructField("RideID", StringType(), True)])

options = {'mode': 'FAILFAST'} # FAILFAST # PERMISSIVE
cleanDF2 = newdf2\
    .selectExpr('CAST(jsonString AS STRING)')\
    .select(from_json('jsonString', schema2).alias('cleandata2'))\
    .select('cleandata2.*')\

# display(cleanDF2)

# COMMAND ----------

# put watermark on both streams

cleanDF1_wtmrk = cleanDF1.withWatermark('Ride_start_time_stamp', '10 seconds')
cleanDF2_wtmrk = cleanDF2.withWatermark('Ride_end_time_stamp', '10 seconds')

# COMMAND ----------

# joins the two stream of data into one dataframe

joined_df = cleanDF1_wtmrk.join(cleanDF2_wtmrk, on='RideID', how='inner')

display(joined_df)

# COMMAND ----------

from pyspark.sql.functions import unix_timestamp, from_unixtime

# convert timestamps to unix to do subtraction (Unix is 32bit integers format, normal is human readable)

df_with_actual_ride_time = joined_df.withColumn('actual_ride_length', unix_timestamp('Ride_end_time_stamp') - unix_timestamp('Ride_start_time_stamp'))

# display(df_with_actual_ride_time)

# COMMAND ----------

# calculate the difference between the ETA and the actual ride length (model bias/error)

df4 = df_with_actual_ride_time.withColumn('difference_in_minutes', df_with_actual_ride_time['Ride_length_estimate_mins'] - df_with_actual_ride_time['actual_ride_length']) 

display(df4)

# COMMAND ----------

# sliding window

spark.conf.set("spark.sql.analyzer.failAmbiguousSelfJoin", "false") 

windowed_df = df4   \
  .withWatermark('Ride_end_time_stamp', '20 seconds') \
  .groupBy( \
    window(df4.Ride_end_time_stamp, '30 minutes', '1 minute'), \
    df4.City) \
  .avg()\

# adding the end time stamp column back in for the dashboard
windowed_df2 = windowed_df.withColumn('Ride_end_time_stamp', F.col('window.end'))

# alias aggregation to snakecase columns (best practice) 
windowed_df3 = windowed_df2.select(col('Ride_end_time_stamp'), col('window'), col('city'), col('avg(difference_in_minutes)').alias('avg_diff_btwn_estimate_and_actual'))

# COMMAND ----------

# access_key = 'AKIA6EW47MOKDA2GXE7A'
# secret_key = '4Bba0ENGCPPDLp6TIy8dgYG0OD5a+JmMSk7yX0m5'
# encoded_secret_key = secret_key.replace("/", "%2F")
# aws_bucket_name = "checkpointbucket2"
# mount_name = "hk-uber-bucket-checkpoints8"

# # TODO: if then statement for mounting 

# directory = dbutils.fs.mount(f"s3a://{access_key}:{encoded_secret_key}@{aws_bucket_name}", f"/mnt/{mount_name}")


# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE if exists uberdataset

# COMMAND ----------

(windowed_df3.writeStream\
  .format("delta")\
  .outputMode("append")\
  .option("checkpointLocation", "checkpointpath")\
  .toTable('example_table')\
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from ;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL 

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE if exists 

# COMMAND ----------

# MAGIC %sql
# MAGIC create table 'example_table' AS select Ride_end_time_stamp, city, avg_diff_btwn_estimate_and_actual
# MAGIC from hive_metastore.default.uberdataset;
# MAGIC
# MAGIC -- this query allows the dataset to be stored in feature store by removing the window'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * 
# MAGIC from ;

# COMMAND ----------

# MAGIC %sh
# MAGIC pip install databricks-feature-store

# COMMAND ----------

# register to feature store

from databricks import feature_store
from pyspark.sql.functions import *

fs = feature_store.FeatureStoreClient()

fs.register_table(
  delta_table= ,
  primary_keys= ,
  description='ETA_bias_by_city'
)

# COMMAND ----------



