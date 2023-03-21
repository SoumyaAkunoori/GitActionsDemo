# Databricks notebook source
from datetime import date
import pandas as pd
from pyspark.sql import SparkSession

# COMMAND ----------

def read_and_combine(Mode,
    data_path, daily_path, feature_path, spark, historical_data_format, date_col
):
    """
    Reads and combines historical and daily data.

    Parameters:
    ----------
    data_path: String
        path to historical data
    daily_path: String
        path to daily data
    feature_path: String
        path to feature data
    spark: spark object
    historical_data_format: String
        format of the data
    date_col: String
        name of the date column
        
    Returns:
    -------
    combined_df: Pandas Dataframe
        Combined Dataframe of historical and Daily data
    """

    #     history = get_data_from_adls(data_path) #### change reading functions
    history = get_data_from_adls(spark, data_path, historical_data_format)
    if  Mode == "Today":
        daily = get_partitioned_data_from_delta(
            spark, daily_path, date.today().strftime("%Y-%m-%d")
        )
    else:
        daily = get_data_from_adls(spark, daily_path)

    h = history.toPandas()
    d = daily.toPandas()

    dates = get_data_from_adls(spark, feature_path)
    date_list = dates.select(date_col).distinct().rdd.map(lambda x: x.Date).collect()
    combined_df = pd.concat([h, d], ignore_index=True)
    if (not isinstance(dates, str)) or (dates is not None):
        date_list = dates.select(date_col).distinct().rdd.map(lambda x: x.Date).collect()
        combined_df = combined_df.loc[~combined_df[date_col].isin(date_list)]
    if len(combined_df) == 0:
        combined_df = "empty"
    return combined_df

# COMMAND ----------

# replacing categorical values to numerical function
def replace_cat(df, cat, target_col):
    """
    used to convert categorical variables into dummy or indicator variables.
    Removes spaces and special character(<) from column heading.

    Parameters:
    ----------
    df: pandas dataframe
        df to store in Datalake
    cat: str
        name of categorical column
    target_col: str
        name of target column

    Returns:
    -------
    df_new: pandas dataframe
        new df with dummy variables
    """
    df_cat = pd.get_dummies(df[cat])
    df_new = pd.concat([df, df_cat], axis=1)
    df_new = df_new.drop([cat], axis=1)
    df_new = df_new.drop([target_col], axis=1)
    df_new.columns = df_new.columns.str.replace(" ", "")
    df_new.columns = df_new.columns.str.replace("<", "")
    return df_new

# COMMAND ----------

def write_features_to_delta(spark,combined_df,categorical_variable,enriched_category,feature_table,feature_path, target_col, primary_key, date_col):
    """
    writes the features to delta table

    Parameters:
    ----------
    spark: spark object
    
    combined_df: pandas dataframe (String in case of no new data)
        combined historical and daily data
    categorical_variable: str
        name of categorical column
    enriched_category: str
        enriched category db name
    feature_table: str
         feature table name
    feature_path: str
        path for feature gen delta table
    target_col: str
        name of target column
    date_col: str
        name of date column

    Returns:
    -------
        None
    """
    if not ((isinstance(combined_df, str)) or (combined_df is None)):
        final = replace_cat(combined_df, categorical_variable, target_col)
        final_spark_df = spark.createDataFrame(final)
        final_spark_df = final_spark_df.dropDuplicates(primary_key)
        table = enriched_category + "." + feature_table
        query = "create database if not exists " + enriched_category
        spark.sql(query)
        write_to_delta_lake(
            spark,
            table,
            final_spark_df,
            feature_path,
            date_col,
            "append",
        )
    else:
        print("no new data")
        logger.log("ML_PIPELINE:no new data to store", "WARN")
