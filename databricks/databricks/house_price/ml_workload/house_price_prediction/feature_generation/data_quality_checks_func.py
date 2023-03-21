# Databricks notebook source
from databricks import feature_store
import databricks.feature_store as FeatureStoreClient
from databricks.feature_store import feature_table
from datetime import date
import pyspark.sql.functions as F
import great_expectations as ge
from pyspark.sql import SparkSession

# COMMAND ----------

# reading features from feature store
def read_features_into_ge(spark, feature_path):
    """
    reads features and stores them in a great expectations dataframe

    Parameters:
    ------------
      spark: spark object
      feature_path: path to features
    Returns:
    ------------
      df_ge: great expectations dataframe
        features in great expectations dataframe
      df_cols: list
        list of columns in the features df
    """

    df = get_data_from_adls(spark, feature_path)
    df_cols = df.columns
    df_ge = ge.dataset.SparkDFDataset(df)
    return df_ge, df_cols

# COMMAND ----------

def check_for_nulls(df_ge, df_cols):
    """
    Checks if null values are present in the columns of given data and prints
    if the check was passed for each column.

    Parameters:
    ----------
    df_ge: great_expectations SparkDFDataset
        df to store in Datalake
    df_cols: python list
        columns in dataframe

    Returns:
    -------
    checks: python list
        List of Boolean
    """
    checks = []
    for col in df_cols:
        try:
            test_result = df_ge.expect_column_values_to_not_be_null(col)
            if test_result.success:
                checks.append(True)
            else:
                checks.append(False)

        except Exception as e:
            raise e

    return checks

# COMMAND ----------

def unique_id_check(df_ge, primary_key):
    """
    Checks if id values of given data are unique and prints if the check was passed.

    Parameters:
    ----------
    df_ge: great_expectations SparkDFDataset
        df having all stored features

    Returns:
    -------
    checks: python list
        List of Boolean
    """
    checks = []
    try:
        test_result = df_ge.expect_compound_columns_to_be_unique(primary_key)
        if test_result.success:
            checks.append(True)
        else:
            checks.append(False)
    except Exception as e:
        print(e)
    return checks

# COMMAND ----------

# exclude date column
def check_cols_are_num(df_ge, col_list):
    """
    Checks if the columns are of numerical type and prints if the check was passed for each column.

    Parameters:
    ----------
    df_ge: great_expectations SparkDFDataset
        df having all stored features
    col_list: python list
        list of numerical column names

    Returns:
    -------
    checks: python list
        List of Boolean
    """
    checks = []
    for col in col_list:
        num_test_result = df_ge.expect_column_values_to_be_in_type_list(
            col, ["IntegerType", "LongType", "FloatType", "DoubleType"]
        )
        try:
            if num_test_result.success:
                checks.append(True)
            else:
                f"{num_test_result.result['unexpected_count']} of {num_test_result.result['element_count']} items in column {column} are not numerical: FAILED"
                checks.append(False)
        except Exception as e:
            print(e)
    return checks

# COMMAND ----------

def check_date_type(df_ge, date_col):
    """
    Checks if date column is of DateType and prints if the check was passed.

    Parameters:
            df_ge: great_expectations SparkDFDataset

    
    Checks if date column is of DateType and prints if the check was passed.
    
    Parameters:
    ----------
    df_ge: great_expectations SparkDFDataset 
        df having all stored features
    
    Returns:
    -------
    checks: python list
        List of Boolean
    """
    type_test_result = df_ge.expect_column_values_to_be_in_type_list(
       date_col, ["DateType"]
    )
    checks = []
    try:
        if type_test_result.success:
            checks.append(True)
        else:
            f"Date column is not of DateType, FAILED"
            checks.append(False)
    except Exception as e:
        print(e)
    return checks

# COMMAND ----------

# def check_data_quality(primry_key,date_col,df_ge, df_cols, logger):
#     num_cols = df_cols.copy()
#     num_cols.remove()  
#     dqchecks = (
#     check_for_nulls(df_ge, df_cols)
#     + unique_id_check(df_ge,primary_key)
#     + check_cols_are_num(df_ge, num_cols)
#     + check_date_type(df_ge, date_col)
#     )
#     for check in dqchecks:
#         if check == True:
#             logger.log("ML_PIPELINE:{} check passed", "INFO")
#             pass
#         else:
#             logger.log("ML_PIPELINE:{} check failed", "ERROR")
#             print(check)
#             raise Exception("Check Failed")

# COMMAND ----------

def check_data_quality(primary_key,date_col,df_ge, df_cols, logger):
    num_cols = df_cols.copy()
    num_cols.remove(date_col)  
    dqchecks = {
    "check_for_nulls":check_for_nulls(df_ge, df_cols),
    "unique_id_check":unique_id_check(df_ge,primary_key),
    "check_cols_are_num": check_cols_are_num(df_ge, num_cols),
    "check_date_type": check_date_type(df_ge, date_col)
    }
    for key in dqchecks:
        for value in dqchecks[key]:
            if value == True:
                logger.log(f"ML_PIPELINE: {key} check passed", "INFO")
#                 print(f"{key} passsed")
                pass
            else:
                logger.log(f"ML_PIPELINE: {key} check failed ", "ERROR")
                print(key)
                raise Exception("Check Failed")
         

# COMMAND ----------


