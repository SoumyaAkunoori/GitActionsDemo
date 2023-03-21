# Databricks notebook source
from datetime import date
import pandas as pd
from pyspark.sql import SparkSession

# COMMAND ----------

# specific to notebooks
dbutils.widgets.dropdown(
    name="Mode",
    defaultValue="Today",
    choices=["Today", "All"],
    label="Feature Generation",
)

# COMMAND ----------

# common widgets
dbutils.widgets.text(
    name="config_file_path",
    defaultValue="../../../../conf/house_price/ml_pipeline_housing_config_new.yml",
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

config_file_path = dbutils.widgets.get("config_file_path")
storage_name = dbutils.widgets.get("storage_name")
scope = dbutils.widgets.get("scope")
log_config_path = dbutils.widgets.get("log_config_file_path")

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

# MAGIC %run ../../../../io_utils

# COMMAND ----------

# MAGIC %run ../../../../logging_utils

# COMMAND ----------

spark1 = SparkSession.builder \
                    .appName('ML_Pipeline') \
                    .getOrCreate()
spark = set_spark_conf(spark1, conf_dict)
config_file_path = dbutils.widgets.get("config_file_path")
app_config = config_load(config_file_path)
log_config = config_load(log_config_path)
logger = Logger(log_config)

# COMMAND ----------

logger.log("ML_PIPELINE:01_Feature Gen NB started", "INFO")

# COMMAND ----------

# MAGIC %run ./feature_gen_func

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reading Data

# COMMAND ----------

today = date.today().strftime("%Y-%m-%d")
target_col = app_config["target_col"]
primary_key = app_config["primary_key"]

# COMMAND ----------

try:
    combined_df = read_and_combine(dbutils.widgets.get("Mode"),
        app_config["data_paths"]["historical_data_path"],
        app_config["data_paths"]["daily_enriched_path"],
        app_config["data_paths"]["feature_path"],
        spark,
        "csv", primary_key[1],
    )
    logger.log("ML_PIPELINE:data for feature generation loaded", "INFO")
except Exception as e:
    logger.log("ML_PIPELINE:error in loading data for feature generation", "ERROR")
    raise (e)


# COMMAND ----------

try:
    write_features_to_delta(spark,combined_df,app_config["categorical_variable"], app_config["databases"]["enriched_db"],app_config["tables"]["feature_table"],app_config["data_paths"]["feature_path"], target_col, app_config['primary_key'],app_config['primary_key'][1])
    logger.log("ML_PIPELINE:features loaded in delta table", "INFO")
except Exception as e:
    logger.log("ML_PIPELINE:error in writing features", "ERROR")
    print(e)
    raise e
