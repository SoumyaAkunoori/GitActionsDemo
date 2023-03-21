# Databricks notebook source
from datetime import date
import databricks.feature_store as FeatureStoreClient
import mlflow.sklearn
import pandas as pd
import pyspark.sql.functions as F
from databricks import feature_store
from mlflow.tracking.client import MlflowClient

# COMMAND ----------

# Get the model URI
def scoring(table_name,primary_key,model_name, date_col,Mode, start_date,end_date, cols, spark):
    """
    fetch features , load model and make inference on daily data.

    Parameters:
    ----------
        None
    Returns:
    -------
        final_df: spark dataframe
    """
    table_name 
    today = date.today().strftime("%Y-%m-%d")
    client = MlflowClient()
    latest_model_version = get_latest_model_version(model_name)
    model_uri = f"models:/{model_name}/{latest_model_version}"
    fs = feature_store.FeatureStoreClient()
    df = fs.read_table(table_name)
    if Mode == "Daily":
        d = df.filter(F.col(date_col) == today)
    else:
        d = df.filter((F.col(date_col) >= start_date) & (F.col(date_col) <= end_date))
    model = mlflow.sklearn.load_model(model_uri=model_uri)
    d = d.toPandas()
    d_id = d[primary_key]
    d = d.drop(primary_key, axis=1)
    x_pred = d.to_numpy()
    y_pred = model.predict(x_pred)
    pred = pd.DataFrame(data=y_pred, columns= cols)
    result = pd.concat([d_id, d, pred], axis=1)
    final_df = spark.createDataFrame(result)
    return final_df
