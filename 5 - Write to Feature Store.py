# Databricks notebook source
# MAGIC %sh
# MAGIC pip install databricks-feature-store

# COMMAND ----------

# register to feature store
# run with Ml cluster 

from databricks import feature_store
from pyspark.sql.functions import *

fs = feature_store.FeatureStoreClient()


fs.register_table(
  delta_table= "hosea.kinesis.average_bias_per_city_per_window_gold",
  primary_keys= "newest_timestamp",
  description='ETA bias per city updated every minute average of past 30 minutes'
) 
