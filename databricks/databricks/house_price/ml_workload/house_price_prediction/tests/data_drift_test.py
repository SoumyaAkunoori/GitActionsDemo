# Databricks notebook source
# MAGIC %run ../data_drift_monitoring/data_drift_func

# COMMAND ----------

import pandas as pd
import numpy as np
import pytest
import ipytest
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
from datetime import date
from pyspark.sql import SparkSession

# COMMAND ----------

# MAGIC %run ../../../../io_utils

# COMMAND ----------

# MAGIC %run ../../../../logging_utils

# COMMAND ----------

# conf_dict = {
#     f"fs.azure.account.auth.type.{storage_name}.dfs.core.windows.net": "OAuth",
#     f"fs.azure.account.oauth.provider.type.{storage_name}.dfs.core.windows.net": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
#     f"fs.azure.account.oauth2.client.id.{storage_name}.dfs.core.windows.net": application_id,
#     f"fs.azure.account.oauth2.client.secret.{storage_name}.dfs.core.windows.net": service_credential,
#     f"fs.azure.account.oauth2.client.endpoint.{storage_name}.dfs.core.windows.net": f"https://login.microsoftonline.com/{directory_id}/oauth2/token",
# }

# COMMAND ----------

# spark1 = SparkSession.builder \
#                     .appName('ML_Pipeline') \
#                     .getOrCreate()
# spark = set_spark_conf(spark1, conf_dict)
# app_config = config_load(config_file_path)
# log_config = config_load(log_config_path)
# logger = Logger(log_config)

# COMMAND ----------

ipytest.autoconfig()

# COMMAND ----------

IntegerType = type(int)

data1 = [["James","H","Smith","36636","M",3000],
    ["Michael","Rose","Phelps","40288","M",4000],
    ["Rebecca","Eva","Williams","42114","F",4000]
  ]
data2 = [("Dave","John","Smith","35636","M",3000),
    ("Luke","Rose","Walter","40258","M",4000),
    ("Mark","A","Williams","42174","F",4000)
  ]

schema = StructType([ \
    StructField("firstname",StringType(),True), \
    StructField("middlename",StringType(),True), \
    StructField("lastname",StringType(),True), \
    StructField("id", StringType(), True), \
    StructField("gender", StringType(), True), \
    StructField("salary", StringType(), True)])
 
df1 = spark.createDataFrame(data=data1,schema=schema)

df2 = spark.createDataFrame(data=data2,schema=schema)

index  = ["firstname","middlename", "lastname","id","gender", "salary"]
exp_df1 = pd.DataFrame(data=data1, columns = index)
exp_df2 = pd.DataFrame(data=data2, columns = index)

cat = "gender"
target = "salary"


# COMMAND ----------

# MAGIC %%ipytest
# MAGIC def test_create_data_frames():
# MAGIC     h, d = create_data_frames(target, df1, spark, "id", df2, "Date")
# MAGIC     assert h == exp_df1
# MAGIC     assert d == exp_df2
# MAGIC test_create_data_frames()
