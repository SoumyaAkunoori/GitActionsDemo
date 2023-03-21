# Databricks notebook source
from datetime import date
import databricks.feature_store as FeatureStoreClient
import mlflow.sklearn
import pandas as pd
import pyspark.sql.functions as F
from databricks import feature_store
from mlflow.tracking.client import MlflowClient
from pyspark.sql import SparkSession

# COMMAND ----------

# common widgets
dbutils.widgets.text(
    name="config_file_path",
    defaultValue="../../../../conf/house_price/ml_pipeline_housing_config.yml",
    label="Config File Path",
)
dbutils.widgets.text(
    name="log_config_file_path",
    defaultValue="../../../../conf/logs/logger_config.yml",
    label="Log config File Path",
)
dbutils.widgets.text(
    name="storage_name",
    defaultValue="mlopscompletepocsa",
    label="Storage Name",
)
dbutils.widgets.text(name="scope", defaultValue="datagen2", label="Databricks Scope")

# COMMAND ----------

# specific to notebook
dbutils.widgets.dropdown(
    name="Mode",
    defaultValue="Date_Range",
    choices=["Daily", "Date_Range"],
    label="scoring",
)
dbutils.widgets.text(
    name="start_date",
    defaultValue="2022-12-07",
    label="Start Date for scoring",
)
dbutils.widgets.text(
    name="end_date", defaultValue="2022-12-14", label="End Date forscoring"
)
start_date = dbutils.widgets.get("start_date")
end_date = dbutils.widgets.get("end_date")

# COMMAND ----------

# MAGIC %run ../../../../io_utils

# COMMAND ----------

# MAGIC %run ../../../../logging_utils

# COMMAND ----------

# MAGIC %run ../utils

# COMMAND ----------

config_file_path = dbutils.widgets.get("config_file_path")
storage_name = dbutils.widgets.get("storage_name")
scope = dbutils.widgets.get("scope")
log_config_path = dbutils.widgets.get("log_config_file_path")
Mode = dbutils.widgets.get("Mode")

# COMMAND ----------

service_credential = dbutils.secrets.get(scope=f"{scope}", key="sp-secret")
application_id = dbutils.secrets.get(scope=f"{scope}", key="application-id")
directory_id = dbutils.secrets.get(scope=f"{scope}", key="directory-id")

# COMMAND ----------

conf_dict = {
    f"fs.azure.account.auth.type.{storage_name}.dfs.core.windows.net": "OAuth",
    f"fs.azure.account.oauth.provider.type.{storage_name}.dfs.core.windows.net": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    f"fs.azure.account.oauth2.client.id.{storage_name}.dfs.core.windows.net": application_id,
    f"fs.azure.account.oauth2.client.secret.{storage_name}.dfs.core.windows.net": service_credential,
    f"fs.azure.account.oauth2.client.endpoint.{storage_name}.dfs.core.windows.net": f"https://login.microsoftonline.com/{directory_id}/oauth2/token",
}

# COMMAND ----------

spark1 = SparkSession.builder \
                    .appName('ML_Pipeline') \
                    .getOrCreate()
spark = set_spark_conf(spark1, conf_dict)
app_config = config_load(config_file_path)
log_config = config_load(log_config_path)
logger = Logger(log_config)

# COMMAND ----------

logger.log("ML_PIPELINE:Scoring NB started", "INFO")

# COMMAND ----------

# MAGIC %run ./scoring_func

# COMMAND ----------

cols = ["predictions"]
table_name = app_config["databases"]['feature_store_db'] + "." + app_config["tables"]['featurestore_table']
final_df = scoring(table_name,app_config['primary_key'],app_config['model_name'],app_config['primary_key'][1],Mode, start_date, end_date, cols, spark)

# COMMAND ----------

query = "create database if not exists " + app_config["databases"]["curated_db"]
spark.sql(query)
table = app_config["databases"]["curated_db"] + "." + app_config["tables"]["scoring_table"]
write_to_delta_lake(
    spark, table, final_df, app_config["data_paths"]["scoring_data_path"], app_config['primary_key'][1], "append"
)

# COMMAND ----------


