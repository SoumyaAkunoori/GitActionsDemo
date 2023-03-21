# Databricks notebook source
import mlflow
from sklearn import linear_model

# COMMAND ----------

def get_scoring_data(train_df_cols,target_col, scoring_data_path):
    """
  gets scored data

  Parameters:
  ----------
    train_df_cols: list
      list of columns in train_df

  Returns:
  -------
    df: spark Dataframe
  """
    df = get_data_from_adls(spark,scoring_data_path)
    df = df.withColumnRenamed("predictions", target_col)
    df = df.select(train_df_cols)
    return df

# COMMAND ----------

def trained_model_coef(model_name):
    """
  gets registered model coefficients.

  Parameters:
  ----------
    None
  Returns:
  -------
    model_coef_: numpy array
      model coefficients
  """
    latest_model_version = get_latest_model_version(model_name)
    model_uri = f"models:/{model_name}/{latest_model_version}"
    model = mlflow.sklearn.load_model(model_uri=model_uri)
    return model.coef_

# COMMAND ----------

def new_model_coef(X_train_np, y_train):
    """
  trains new model including scoring data and generates model coefficient

  Parameters:
  ----------
    X_train_np: numpy array of predictors in  training data
    y_train: target value of training data
  Returns:
  -------
    lr_coef_: numpy array
      model coefficients
  """
    lr = linear_model.LinearRegression().fit(X_train_np, y_train)
    return lr.coef_

# COMMAND ----------

def check_sign(arr):
    """
  assigns 0 or 1 based upon positive or negative values.

  Parameters:
  ----------
    arr: numpy array
  Returns:
  -------
    arr: numpy array
      with 0 and 1 values
  """
    li = arr
    for i in range(len(arr)):
        if arr[i] > 0:
            li[i] = 1
        else:
            li[i] = 0
    return li
