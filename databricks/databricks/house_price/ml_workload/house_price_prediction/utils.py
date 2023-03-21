# Databricks notebook source
import os.path as op
from pyspark.sql.functions import col
import yaml
from databricks.feature_store import FeatureLookup
import numpy as np
import pandas as pd
from databricks import feature_store
import databricks.feature_store as FeatureStoreClient
from sklearn.model_selection import train_test_split
from mlflow.tracking import MlflowClient


# COMMAND ----------

def req_cols(df,primary_key,id_col, target_col):
    """
    Selects only the required columns to join it with feature store table.
    
    Parameters:
    ----------
    df : spark dataframe 
        daily enriched data
    primary_key: Python List
        primary key columns as used in feature store table creation
    id_col: String
        id column name
    target_col: target col name
    Returns:
    -------
        n_df: spark dataframe
    """
    n_df = df.select(primary_key + [target_col] ).withColumn(id_col, col(id_col).cast('Long'))
    return n_df

# COMMAND ----------

def train_test_split_data(df):
  """
  splits data in train and test

  Parameters:
  ----------
    df: PySpark dataframe , containing training data

  Returns:
  -------
    X_train_np: numpy array
    y_train: pandas series
    X_test_np: numpy array
    y_test: pandas series
  """
  features_and_label = df.columns

  # Collect data into a Pandas array for training
  data = df.toPandas()[features_and_label]
  X_train, X_test, y_train, y_test = train_test_split(data.drop(columns = [target_col], axis = 1), data[target_col], test_size=0.2, random_state = 0)

  X_train_np = X_train.to_numpy()
  X_test_np = X_test.to_numpy()
  return X_train_np, y_train, X_test_np, y_test


# COMMAND ----------

def get_training_data(df,table_name,primary_key ):
  """
  creating training data

  Parameters:
  ----------
      df: pyspark dataframe contains training data
      table_name: feature store table name
      primary_key: primary key for feature store
  Returns:
  -------
      df: spark dataframe
  """
  feature_lookups = [
    FeatureLookup(
      table_name = table_name,
      lookup_key = primary_key
    )
  ]

  fs = feature_store.FeatureStoreClient()

  training_set = fs.create_training_set(
    df=df,
    feature_lookups = feature_lookups,
    label = target_col,
    exclude_columns = primary_key
  )

  training_df = training_set.load_df()
  return training_df

# COMMAND ----------

def get_latest_model_version(model_name):
    '''
     fetch the latest model  
     
     Parameters:
    ----------
        None
    Returns:
    -------
        latest_version: latest version
  
    '''
    latest_version = 1
    mlflow_client = MlflowClient()
    for mv in mlflow_client.search_model_versions(f"name='{model_name}'"):
        version_int = int(mv.version)
        if version_int > latest_version:
              latest_version = version_int
    return latest_version
