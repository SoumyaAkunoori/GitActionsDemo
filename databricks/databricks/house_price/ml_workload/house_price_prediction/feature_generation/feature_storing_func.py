# Databricks notebook source
from databricks import feature_store
from pyspark.sql import SparkSession

# COMMAND ----------

# MAGIC %run ../../../../io_utils

# COMMAND ----------

from databricks import feature_store


def store_features(primary_key, ft_tb,ft_db,feature_path,spark):

    """
    Creates feature store table and updates it based on primary key.

    Parameters:
    ----------
        primary_key: String
            list having column names of primary key
        ft_tb: String
            feature store table name
        ft_db: String
            feature store db name
        feature_path: String
            path to feature store delta table
        spark: spark Object
        
    Returns:
    -------
      None
    """
    df = get_data_from_adls(spark, feature_path)
    query = "create database if not exists " + ft_db
    spark.sql(query)
    fs = feature_store.FeatureStoreClient()
    spark.conf.set("spark.sql.shuffle.partitions", "5")
    ft_table = ft_db + "." + ft_tb
    fs.create_table(
        name=ft_table, primary_keys=primary_key, df=df, description=ft_db
    )
    fs.write_table(name=ft_table, df=df, mode="merge")
