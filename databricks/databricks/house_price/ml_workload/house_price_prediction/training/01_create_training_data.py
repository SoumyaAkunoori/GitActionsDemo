# Databricks notebook source
from pyspark.sql import SparkSession

# COMMAND ----------

# MAGIC %run ../utils

# COMMAND ----------

dbutils.widgets.text(
    name="start_date",
    defaultValue="2022-12-07",
    label="Start Date for training",
)
dbutils.widgets.text(
    name="end_date", defaultValue="2022-12-14", label="End Date for training"
)
dbutils.widgets.dropdown(
    name="Mode",
    defaultValue="all",
    choices=["historical", "all", "in given range"],
    label="Training Data",
)


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

storage_name = dbutils.widgets.get("storage_name")
scope = dbutils.widgets.get("scope")
log_config_path = dbutils.widgets.get("log_config_file_path")
Mode = dbutils.widgets.get("Mode")
start_date = dbutils.widgets.get("start_date")
end_date = dbutils.widgets.get("end_date")

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

logger.log("ML_PIPELINE:01_Create_Training_Data started", "INFO")

# COMMAND ----------

# MAGIC %run ./create_training_data_func

# COMMAND ----------

daily_enriched_data = app_config["data_paths"]["daily_enriched_path"]
table_name = app_config["databases"]["enriched_db"] + "." + app_config["tables"]["training_table"]

# COMMAND ----------

try:
    hist_data_path = app_config["data_paths"]["historical_data_path"]
    retraining_data(spark,daily_enriched_data,"csv", hist_data_path, app_config["primary_key"],app_config['primary_key'][0],app_config['primary_key'][1],app_config['target_col'],table_name,app_config["data_paths"]["training_path"],Mode,start_date,end_date)
    logger.log("ML_PIPELINE:Created training data successfully", "INFO")
except Exception as e:
    logger.log("ML_PIPELINE:Error while creating training data", "ERROR")
    raise e

# COMMAND ----------

logger.log("ML_PIPELINE:01_Create_Training_Data completed", "INFO")
