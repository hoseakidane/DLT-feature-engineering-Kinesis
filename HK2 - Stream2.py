# Databricks notebook source
# MAGIC %sh
# MAGIC pip install dbldatagen

# COMMAND ----------

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

# define schema for fake data
schema = StructType([
    StructField("RideID", StringType(), True)])

# Create datagen object with a table name and pass in schema, rows, partition and end_time_stamp
ds = (dg.DataGenerator(spark, name="end_time_fake_data_stream", rows=100000, partitions=20).withColumn('Ride_end_time_stamp', 'timestamp', begin='2023-01-02 00:9:10', end="2026-12-31 00:00:00", interval="5 minute").withSchema(schema))

# turn the datagen object into a streaming dataframe 
df = ds.build(withStreaming=True, options={'rowsPerSecond': 1})

# convert df to dictionary before inputing into json function
df1 = df.withColumn('json', create_map(lit('Ride_end_time_stamp'),col('Ride_end_time_stamp'), lit('RideID'),col('RideID')))

# display(df1)

# COMMAND ----------

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

# COMMAND ----------


