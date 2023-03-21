# Databricks notebook source
# MAGIC %run ../../../../../io_utils

# COMMAND ----------

# MAGIC %run ../../../../../logging_utils

# COMMAND ----------

# MAGIC %run ./01_data_generation_func

# COMMAND ----------

import pyspark
import pandas as pd
import yaml
from pyspark.sql.types import *
import numpy as np
import pyspark.sql.functions as F
import dbldatagen as dg
import dbldatagen.daterange
from datetime import date,timedelta
import os

# COMMAND ----------

dbutils.widgets.dropdown(name='Mode',defaultValue='Daily',choices=['Daily','Date_Range'],label='Data Generation')
dbutils.widgets.text(name='start_date',defaultValue='2022-12-07',label='Start Date for Data Generation')
dbutils.widgets.text(name='end_date',defaultValue='2022-12-14',label='End Date for Data Generation')
dbutils.widgets.text(
    name="config_file_path",
    defaultValue="../../../../../conf/house_price/housing_config.yml",
    label="Config File Path",
)
dbutils.widgets.text(
    name="log_config_file_path",
    defaultValue="../../../../../conf/logs/logger_config.yml",
    label="Log config File Path",
)
dbutils.widgets.text(
    name="storage_name",
    defaultValue="mlopscompletepocsa",
    label="Storage Name",
)
dbutils.widgets.text(
    name="scope", defaultValue="datagen2", label="Databricks Scope"
)

# COMMAND ----------

start_date = dbutils.widgets.get('start_date')
end_date = dbutils.widgets.get('end_date')
config_file_path = dbutils.widgets.get("config_file_path")
log_config_file_path = dbutils.widgets.get("log_config_file_path")
storage_name = dbutils.widgets.get("storage_name")
scope = dbutils.widgets.get("scope")

# COMMAND ----------

service_credential = dbutils.secrets.get(scope=f"{scope}", key="sp-secret")
application_id = dbutils.secrets.get(scope=f"{scope}", key="application-id")
directory_id = dbutils.secrets.get(scope=f"{scope}", key="directory-id")

# COMMAND ----------

conf_dict = {
    f"fs.azure.account.auth.type.{storage_name}.dfs.core.windows.net":"OAuth",
    f"fs.azure.account.oauth.provider.type.{storage_name}.dfs.core.windows.net":
    "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    f"fs.azure.account.oauth2.client.id.{storage_name}.dfs.core.windows.net":application_id,
    f"fs.azure.account.oauth2.client.secret.{storage_name}.dfs.core.windows.net":service_credential,
    f"fs.azure.account.oauth2.client.endpoint.{storage_name}.dfs.core.windows.net":
    f"https://login.microsoftonline.com/{directory_id}/oauth2/token",
}

# COMMAND ----------

spark = set_spark_conf(spark,conf_dict)

# COMMAND ----------

# load config files
config = config_load(config_file_path)
log_config = config_load(log_config_file_path)

# COMMAND ----------

#create logger object
logger = Logger(log_config)

# COMMAND ----------

raw_csv_path = config['raw_csv_path']
raw_data_path = config['raw_data_path']
db = config['database']
table = config['raw_table']

# COMMAND ----------

logger.log("Starting Data Generation","INFO")

# COMMAND ----------

logger.log("Reading Raw CSV file From ADLS","INFO")
df = get_data_from_adls(spark, raw_csv_path, "csv")

# COMMAND ----------

min_max_dict,col_types_list = fetch_min_max(df)

# COMMAND ----------

try:
    get_partitioned_data_from_delta(spark,raw_data_path,date.today().strftime('%Y-%m-%d'))
    path_exist = True
except Exception as e:
    path_exist = False

if dbutils.widgets.get('Mode') == 'Daily':
    write_data_daily(spark,path_exist)
else:
    write_data_date_range(spark,path_exist)

# COMMAND ----------

logger.log("Data Generation Completed","INFO")
