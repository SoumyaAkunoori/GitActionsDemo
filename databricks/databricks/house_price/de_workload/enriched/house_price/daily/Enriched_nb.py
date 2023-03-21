# Databricks notebook source
# MAGIC %run ../../../../../io_utils

# COMMAND ----------

# MAGIC %run ../../../../../logging_utils

# COMMAND ----------

# MAGIC %run ./enriched_nb_func

# COMMAND ----------

import os
from datetime import date,timedelta
from pyspark.sql.functions import col, count, isnull, when

# COMMAND ----------

dbutils.widgets.dropdown(name='Mode',defaultValue='Today',choices=['Today','All'],label='Data Cleaning')
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

raw_data_path = config['raw_data_path']
enriched_data_path = config['enriched_data_path']
db = config['database']
table = config['enriched_table']

# COMMAND ----------

logger.log("Starting Data Cleaning","INFO")

# COMMAND ----------

if dbutils.widgets.get('Mode') == 'Today':
    df = get_partitioned_data_from_delta(spark,raw_data_path,date.today().strftime('%Y-%m-%d'))
    
else:
    df = get_data_from_adls(spark,raw_data_path)

# COMMAND ----------

#data cleaning
df = remove_duplicates(df)
df = remove_null_columns(df)
df = remove_null_rows(df)

# COMMAND ----------

try:
    get_partitioned_data_from_delta(spark,enriched_data_path,date.today().strftime('%Y-%m-%d'))
    path_exist = True
except Exception as e:
    path_exist = False
    
#Write data to enriched layer
if dbutils.widgets.get('Mode') == 'Today':
    data_cleaning_daily(spark,f"{db}.{table}",df,enriched_data_path,"Date","append")
else:
    data_cleaning_all(spark,f"{db}.{table}",df,enriched_data_path,"Date","overwrite")

# COMMAND ----------

logger.log("Data Cleaning Done","INFO")

# COMMAND ----------


