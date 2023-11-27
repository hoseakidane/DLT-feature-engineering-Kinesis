# Databricks notebook source
dbutils.widgets.remove('STREAM1')
dbutils.widgets.text("STREAM_NAME1", "Enter a name for your stream")
dbutils.widgets.text("STREAM_NAME2", "Enter a name for your stream")
dbutils.widgets.text('REGION_NAME', "Enter the region of your stream")
dbutils.widgets.text("aws_access_key_id", "Enter your access key")
dbutils.widgets.text("aws_secret_key", "Enter your secret key")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DROP TABLE IF EXISTS hosea.kinesis.delta_stream1;
# MAGIC DROP TABLE IF EXISTS hosea.kinesis.delta_stream2; 

# COMMAND ----------

# MAGIC %sql
# MAGIC -- creates catalog and schema for downstream tables 
# MAGIC
# MAGIC CREATE CATALOG IF NOT EXISTS hosea;
# MAGIC CREATE SCHEMA IF NOT EXISTS hosea.kinesis;
# MAGIC

# COMMAND ----------

import boto3

# creates 2 kinesis streams programtically 
# replace the following four variables with the info about your kinesis stream
# You may hard-code for development but using secret scopes is recommended

STREAM_NAME1 = dbutils.widgets.get('STREAM_NAME1')
STREAM_NAME2 = dbutils.widgets.get('STREAM_NAME2')
REGION_NAME = dbutils.widgets.get('REGION_NAME')
aws_access_key_id = dbutils.widgets.get('aws_access_key_id')
aws_secret_key = dbutils.widgets.get('aws_secret_key')

session = boto3.Session(
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_key,
    region_name=REGION_NAME
)
# creating streams in root account instead of iam user for some reason (need to fix)
kinesis = session.client('kinesis')
kinesis.create_stream(StreamName=STREAM_NAME1, ShardCount=1)
kinesis.create_stream(StreamName=STREAM_NAME2, ShardCount=1)
