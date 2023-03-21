# Databricks notebook source
# MAGIC %run ../../../../../../io_utils

# COMMAND ----------

# MAGIC %run ../../../../../../logging_utils

# COMMAND ----------

# MAGIC %run ./Data_Cleaning_utils

# COMMAND ----------

from pyspark.sql.functions import col, count, isnull, when

# COMMAND ----------

dbutils.widgets.text(
    name="config_file_path",
    defaultValue="../../../../../../conf/tpc_ds/enriched-de-workload.yml",
    label="Config File Path",
)
dbutils.widgets.text(
    name="log_config_file_path",
    defaultValue="../../../../../../conf/logs/logger_config.yml",
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
storage_name = dbutils.widgets.get("storage_name")
scope = dbutils.widgets.get("scope")
log_config_file_path = dbutils.widgets.get("log_config_file_path")

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

# Path to raw data
input_container_path = config["adls"]["input_container_path"]

# Output Path of enriched data
output_container_path = config["adls"]["output_container_path"]

# Database Tables
tables = config["tables"]

# Database to store enriched tables
db = config["adls"]["database"]


# COMMAND ----------

logger.log("Starting Data Cleaning of Raw Data","INFO")

# COMMAND ----------

# Read data from Raw layer
logger.log("Reading Raw Data From ADLS","INFO")
for table in tables:
    globals()[table] = get_data_from_adls(spark, f"{input_container_path}/{table}")

# COMMAND ----------

# data cleaning
for table in tables:
    globals()[table] = remove_duplicates(globals()[table])
    globals()[table] = remove_null_columns(globals()[table])
    globals()[table] = remove_null_rows(globals()[table])

# COMMAND ----------

# Write data into datalake
logger.log("Writing Data to Datalake Enriched layer","INFO")
for table in tables:
    write_to_delta_lake(spark, f"{db}.{table}", globals()[table], f"{output_container_path}/{table}")
logger.log("Multiple data sources detected for transfer","WARN")

# COMMAND ----------

logger.log("Data Cleaning completed","INFO")
