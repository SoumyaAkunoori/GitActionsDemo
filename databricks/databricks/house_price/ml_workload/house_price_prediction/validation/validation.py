# Databricks notebook source
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error
import mlflow
import math
from pyspark.sql import SparkSession

# COMMAND ----------

# MAGIC %run ../../../../io_utils

# COMMAND ----------

# MAGIC %run ../../../../logging_utils

# COMMAND ----------

# MAGIC %run ../utils

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

# COMMAND ----------

app_config = config_load(config_file_path)
log_config = config_load(log_config_path)
logger = Logger(log_config)

# COMMAND ----------

df = get_data_from_adls(spark,app_config["training_path"])
target_col = app_config["target_col"]
primary_key = app_config["primary_key"]
table_name = (
    app_config["feature_store_db_name"]
    + "."
    + app_config["featurestore_table"]
)

train_df = get_training_data(df, table_name, primary_key)
X_train_np, y_train, X_test_np, y_test = train_test_split_data(train_df)


# COMMAND ----------

def load_model():
    """
    Loads model from mlflow and returns
    
    Parameters:
    ----------
        None
    Returns:
    -------
        Model
    """
    model_name = app_config["model_name"]
    model_version_uri = "models:/{model_name}/latest".format(model_name=model_name)
    print("Loading registered model version from URI: '{model_uri}'".format(model_uri=model_version_uri))
    model = mlflow.pyfunc.load_model(model_version_uri)
    return model

# COMMAND ----------

def calculate_mse(actual, prediction):
    """
    Calculates MSE
    
    Parameters:
    ----------
        actual : np.array
        prediction : np.array
    Returns:
    -------
        mse : float
    """
    return mean_squared_error(y_test, prediction)

# COMMAND ----------

model = load_model()
prediction = model.predict(X_test_np)

# COMMAND ----------

mse = calculate_mse(y_test, prediction)
mse

# COMMAND ----------

rmse = math.sqrt(mse)
rmse

# COMMAND ----------

validation_ = True if rmse<2500 else False

# COMMAND ----------

dbutils.notebook.exit(validation_)
