# Databricks notebook source
from databricks import feature_store
from pyspark.sql import SparkSession

# COMMAND ----------

# MAGIC %run ../../../../logging_utils

# COMMAND ----------

# MAGIC %run ../../../../io_utils

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

spark1 = SparkSession.builder \
                    .appName('ML_Pipeline') \
                    .getOrCreate()
spark = set_spark_conf(spark1, conf_dict)
app_config = config_load(config_file_path)
log_config = config_load(log_config_path)
logger = Logger(log_config)

# COMMAND ----------

logger.log("ML_PIPELINE:03_feature_store NB started", "INFO")

# COMMAND ----------

# MAGIC %run ./feature_storing_func

# COMMAND ----------

ft_db = app_config["databases"]["feature_store_db"]
ft_tb = app_config["tables"]["featurestore_table"]
primary_key = app_config["primary_key"]


# COMMAND ----------

try:
    store_features(primary_key, ft_tb, ft_db,app_config["data_paths"]['feature_path'] ,spark)
    logger.log("ML_PIPELINE:Read features for storing in feature store", "INFO")
except Exception as e:
    logger.log("ML_PIPELINE:error in storing features", "ERROR")
    print(e)
    raise e
