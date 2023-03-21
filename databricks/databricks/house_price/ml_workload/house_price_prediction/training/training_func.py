# Databricks notebook source
import databricks.feature_store as FeatureStoreClient
import mlflow
import numpy as np
import pandas as pd
import pyspark.sql.functions as F
import yaml
from mlflow.tracking import MlflowClient
from sklearn import linear_model
from sklearn.metrics import mean_squared_error

def training(X_train_np, y_train, X_test_np, y_test, run_name, artifact_path, metric, model_name):
    """
    fetch features , trains the model on training dataset
    and register model in MLflow

    Parameters:
    ----------
        X_train_np: numpy array
        y_train: pandas series
        X_test_np: numpy array
        y_test: pandas series
    Returns:
    -------
        None
    """
    mlflow.sklearn.autolog()

    with mlflow.start_run(run_name= run_name) as run:
        mlflow.sklearn.autolog()
        lr = linear_model.LinearRegression().fit(X_train_np, y_train)
        y_pred = lr.predict(X_test_np)
        rmse = mean_squared_error(y_test, y_pred)
        mlflow.log_metric(metric, rmse)
        mlflow.sklearn.log_model(
            sk_model=lr,
            artifact_path= artifact_path,
            registered_model_name=model_name,
        )
