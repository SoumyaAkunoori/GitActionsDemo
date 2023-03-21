# Databricks notebook source
# MAGIC %run ../feature_generation/feature_gen_func

# COMMAND ----------

import pandas as pd
import numpy as np
import pytest
import ipytest
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
from datetime import date

# COMMAND ----------

ipytest.autoconfig()

# COMMAND ----------

# specific to notebooks
dbutils.widgets.dropdown(
    name="Mode",
    defaultValue="Today",
    choices=["Today", "All"],
    label="Feature Generation",
)

# COMMAND ----------

# # common widgets
# dbutils.widgets.text(
#     name="config_file_path",
#     defaultValue="../../../../conf/house_price/ml_pipeline_housing_config.yml",
#     label="Config File Path",
# )
# dbutils.widgets.text(
#     name="log_config_file_path",
#     defaultValue="../../../../conf/logs/logger_config.yml",
#     label="Log config File Path",
# )
# dbutils.widgets.text(
#     name="storage_name",
#     defaultValue="mlopscompletepocsa",
#     label="Storage Name",
# )
# dbutils.widgets.text(name="scope", defaultValue="datagen2", label="Databricks Scope")

# COMMAND ----------

IntegerType = type(int)

data2 = [("James","","Smith","36636","M",3000),
    ("Michael","Rose","","40288","M",4000),
    ("Rebecca","","Williams","42114","F",4000)
  ]

schema = StructType([ \
    StructField("firstname",StringType(),True), \
    StructField("middlename",StringType(),True), \
    StructField("lastname",StringType(),True), \
    StructField("id", StringType(), True), \
    StructField("gender", StringType(), True), \
    StructField("salary", StringType(), True)])
 
df = spark.createDataFrame(data=data2,schema=schema)
cat = "gender"
target = "salary"
data_expected = [("James","","Smith","36636",0,1),
    ("Michael","Rose","","40288",0,1),
    ("Rebecca","","Williams","42114",1,0)
  ]
schema1 = StructType([ \
    StructField("firstname",StringType(),True), \
    StructField("middlename",StringType(),True), \
    StructField("lastname",StringType(),True), \
    StructField("id", StringType(), True), \
    StructField("M", StringType(), True), \
    StructField("F", StringType(), True)])

expected_result = spark.createDataFrame(data_expected,schema=schema1)
expected_result = expected_result.drop("gender")
expected_result = expected_result.drop("salary")

# COMMAND ----------

# %%ipytest
# def test_read_and_combine():
    
#     result = read_and_combine(data_path,daily_path,feature_path, data_format)
# #     print(type(latest_version))
#     assert type(latest_version) == IntegerType

#     test_read_and_combine()

# COMMAND ----------

# MAGIC %%ipytest
# MAGIC def test_replace_cat():
# MAGIC     
# MAGIC     result = replace_cat(df, cat, target)
# MAGIC #     print(type(latest_version))
# MAGIC     expected = expected_result
# MAGIC     print(result)
# MAGIC     assert result == expected
# MAGIC test_replace_cat()

# COMMAND ----------


