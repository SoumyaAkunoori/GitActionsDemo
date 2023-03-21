# Databricks notebook source
# MAGIC %md
# MAGIC ## Check for Nulls

# COMMAND ----------

def dq_null_check(df_ge,df_cols):
    """
    Checks for null values in columns
  
    Parameters:
    ----------
    table_list : list 
        Name of the tables.
    
    Returns:
    -------
    None
    """
    for column in df_cols:
            try:
                test_result = df_ge.expect_column_values_to_not_be_null(column)
                if test_result.success:
                    print(f"All items in column {column} are not null: PASSED")

                else:
                    print(f"Uh oh! {test_result.result['unexpected_count']} of {test_result.result['element_count']} items in column {column} are null: FAILED")
                    logger.log(f"Uh oh! {test_result.result['unexpected_count']} of {test_result.result['element_count']} items in column {column} are null: FAILED","ERROR")
            except Exception:
                logger.log("Error trying to perform null check", "ERROR")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check if columns are numerical

# COMMAND ----------

#exclude date column
def check_cols_are_num(df_ge, num_cols):
    """
    Checks if the columns are of numerical type and prints if the check was passed for each column.
    
    Parameters:
    ----------
    df_ge: great_expectations SparkDFDataset 
        df having all stored features
    col_list: python list
        list of column names
    
    Returns:
    -------
    None
    """
    for col in num_cols:
        num_test_result = df_ge.expect_column_values_to_be_in_type_list(col, ["IntegerType","LongType","FloatType","DoubleType"])
        try:
            if num_test_result.success:
                print(f" {col} is numerical: PASSED")
                #logger.log(f" {col} is numerical: PASSED","INFO")
            else:  
                print(f"{num_test_result.result['unexpected_count']} of {num_test_result.result['element_count']} items in column {column} are not numerical: FAILED")
                logger.log(f"{num_test_result.result['unexpected_count']} of {num_test_result.result['element_count']} items in column {column} are not numerical: FAILED","ERROR")
        except Exception:
            logger.log("Error trying to perform numerical column check", "ERROR")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check Date column type

# COMMAND ----------

def check_date_type(df_ge):
    '''
        Checks if date column is of DateType and prints if the check was passed.
        
        Parameters: 
                df_ge: great_expectations SparkDFDataset

    '''
    """
    Checks if date column is of DateType and prints if the check was passed.
    
    Parameters:
    ----------
    df_ge: great_expectations SparkDFDataset 
        df having all stored features
    
    Returns:
    -------
    None
    """
    type_test_result = df_ge.expect_column_values_to_be_in_type_list("Date", ["DateType"])
    try:
        if type_test_result.success:
            print("Date column is of type - DateType: PASSED")
            #logger.log("Date column is of type - DateType: PASSED","INFO")
            
        else:
            print(f"Date column is not of DateType, FAILED")
            logger.log(f"Date column is not of DateType, FAILED","ERROR")
    except Exception:
        logger.log("Error trying to perform date check","ERROR")
