# Databricks notebook source
import databricks.feature_store as FeatureStoreClient
import pyspark.sql.functions as F
from databricks import feature_store
from datetime import date
from alibi_detect.cd import  TabularDrift

# COMMAND ----------

def create_data_frames(target_col, table_name, spark, primary_key, training_path, date_col):
    """
    Reads features from feature store and divides data
    in reference and test dataframe
    Parameters:
    ----------
        target_col: String
            name of the target feature 
        table_name: String
            delta table name of training data
        spark: spark object
            spark object for session
        primary_key: List of Strings
            list of primary key column names 
        training_path: String
            training data path
        date_col: String
            name of date col
    Returns:
    -------
        h,d: Pandas Dataframes
    """
    today = date.today().strftime("%Y-%m-%d")
    fs = feature_store.FeatureStoreClient()
    df = fs.read_table(table_name)
    train_df = get_data_from_adls(spark,training_path)
    train_df = train_df.select(primary_key)
    h = df.join(train_df, on= primary_key, how="left")
    h = h.select(df.columns)
    d = df.filter(F.col(date_col) == today)
    h = h.toPandas()
    d = d.toPandas()
    return h, d

# COMMAND ----------

def tabular_drift(x_ref,x_test, p_val, categories_per_feature):
    """
    predicts drift between reference and test data
    Parameters:
    ----------
        x_ref: pandas dataframe
             reference dataset
        x_test: pandas dataframe
             test dataset
         p_val: float
             p_value threshold
         categories_per_feature: dictionary
             key - column number of categorical varaibles, value- list (possible set of values) 
    Returns:
    -------
        results: Boolean 
            True if drift occurs else False
    """
    cd = TabularDrift(
    x_ref.to_numpy(),
    p_val=p_val,
    categories_per_feature= categories_per_feature,)
    return cd.predict(x_test.to_numpy())
