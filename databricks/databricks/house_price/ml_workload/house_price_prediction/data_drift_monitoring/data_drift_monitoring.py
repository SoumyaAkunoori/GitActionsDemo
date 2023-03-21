# Databricks notebook source
import databricks.feature_store as FeatureStoreClient
import pyspark.sql.functions as F
from databricks import feature_store
from datetime import date
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
dbutils.widgets.text(
    name="scope", defaultValue="datagen2", label="Databricks Scope"
)

# COMMAND ----------

# MAGIC %run ../../../../io_utils

# COMMAND ----------

# MAGIC %run ../../../../logging_utils

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
    f"fs.azure.account.auth.type.{storage_name}.dfs.core.windows.net":"OAuth",
    f"fs.azure.account.oauth.provider.type.{storage_name}.dfs.core.windows.net":
    "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    f"fs.azure.account.oauth2.client.id.{storage_name}.dfs.core.windows.net":application_id,
    f"fs.azure.account.oauth2.client.secret.{storage_name}.dfs.core.windows.net":service_credential,
    f"fs.azure.account.oauth2.client.endpoint.{storage_name}.dfs.core.windows.net":
    f"https://login.microsoftonline.com/{directory_id}/oauth2/token",
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

logger.log("ML_PIPELINE:data drift monitoring NB started","INFO")

# COMMAND ----------

# MAGIC %run ./data_drift_func

# COMMAND ----------

target_col= app_config["target_col"]
primary_key = app_config["primary_key"]
training_path = app_config["data_paths"]["training_path"]
date_col = primary_key[1]

# COMMAND ----------

ft_db = app_config["databases"]["feature_store_db"]
table_name = ft_db + "." + app_config["tables"]["featurestore_table"]
x_ref, x_test = create_data_frames(target_col, table_name, spark, primary_key, training_path, date_col)

# COMMAND ----------

categories_per_feature={
        10: [0, 1],
        11: [0, 1],
        12: [0, 1],
        13: [0, 1],
        14: [0, 1],
    }
preds = tabular_drift(x_ref,x_test,0.5,categories_per_feature)

# COMMAND ----------

labels = ["No!", "Yes!"]
print("Drift? {}".format(labels[preds["data"]["is_drift"]]))

# COMMAND ----------

dbutils.notebook.exit(preds["data"]["is_drift"])

# COMMAND ----------


