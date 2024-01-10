# Databricks notebook source
# MAGIC %sh
# MAGIC pip install dbldatagen

# COMMAND ----------

# generate fake data for stream

from datetime import timedelta, datetime
import math
import dbldatagen as dg
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from boto3.session import Session
from pyspark.sql import DataFrame
from pyspark.sql import Row
import boto3
import json
from pyspark.sql.window import *

# define dataframe schema for fake data
schema = StructType([
    StructField("RideID", IntegerType(), True),
    StructField("Ride_length_estimate_mins", IntegerType(), True),
    StructField("City", StringType(), True),
])

# Create datagen object with a table name and pass in schema, rows, partition and timestamp column (workaround since it kept giving me a null value if i overwrite the column using the withcolumnspe method)
ds = (dg.DataGenerator(spark, name="Start_time_fake_data_stream", rows=100000, partitions=20).withColumn('Ride_start_time_stamp', 'timestamp', begin='2023-01-02 00:09:00', end="2023-12-31 00:00:00", interval="5 minute")
      .withSchema(schema)
      )

# specify a list of values to randomize through for the city column  
ds2 = ds.withColumnSpec('Ride_length_estimate_mins', minValue=10, maxValue=15, random=True).withColumnSpec('City', values=['SF', 'LA', 'NYC', 'Miami', 'Austin', 'Chicago', 'Detroit',])

# turn the datagen object into a streaming dataframe 
df = ds2.build(withStreaming=True, options={'rowsPerSecond': 1})

# convert df to dictionary before inputing into json function 
df1 = df.withColumn('json', create_map(lit('Ride_start_time_stamp'),col('Ride_start_time_stamp'), lit('RideID'),col('RideID'), lit('Ride_length_estimate_mins'),col('Ride_length_estimate_mins'), lit('City'),col('City')))

# display(df1)


# COMMAND ----------

# write to kinesis with foreachbatch 

spark = SparkSession.builder.getOrCreate()

class RowPrinter:
  kinesis_client = None
  
  def open(self, partition_id, epoch_id):
    print("Opened %d, %d" % (partition_id, epoch_id))
    
    STREAM_NAME = 
    REGION_NAME = 
    aws_access_key_id = 
    aws_secret_key = 

    self.kinesis_client = boto3.client("kinesis", region_name=REGION_NAME,  aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_key)
    return True
  
  def process(self, row):
    STREAM_NAME = 
    payload = json.dumps(row.asDict(), default=str)
    
    response = self.kinesis_client.put_record(StreamName=STREAM_NAME, Data=payload, PartitionKey="partitionkey")
    print(response)
  
  def close(self, error):
    print("Closed with error: %s" % str(error))
  
stream = df1.writeStream.foreach(RowPrinter()).outputMode("append").start()

