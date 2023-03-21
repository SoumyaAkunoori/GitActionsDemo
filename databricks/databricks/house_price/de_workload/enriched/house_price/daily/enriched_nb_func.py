# Databricks notebook source
def remove_duplicates(table):
    """
    Removes duplicate rows from table
    
    Parameters:
    ----------
    table : Pyspark Dataframe(pyspark.sql.dataframe.DataFrame)
        Name of the dataframe 
        
    Returns:
    -------
    Pyspark Dataframe(pyspark.sql.dataframe.DataFrame)
    """
    try:
        return table.dropDuplicates()
    except Exception:
        logger.log("Error while removing duplicates from table","ERROR")

# COMMAND ----------

def remove_null_columns(table):
    """
    Removes columns with all null values.
    
    Parameters:
    ----------
    table : Pyspark Dataframe(pyspark.sql.dataframe.DataFrame)
        Name of the dataframe 
        
    Returns:
    -------
    Pyspark Dataframe(pyspark.sql.dataframe.DataFrame)
    """
    try:
        colsthatarenull = (
            table.select(
                [(when(isnull(c), c)).alias(c) for c in table.columns]
            )
            .first()
            .asDict()
        )
        namesofnullcols = {
            key: val for key, val in colsthatarenull.items() if val != None
        }.values()
        df = table.select(
            [count(when(col(c).isNull(), c)).alias(c) for c in namesofnullcols]
        )
        rows = table.count()
        colstodrop = []
        for c in namesofnullcols:
            if df.select(c).first()[0] == rows:
                colstodrop.append(c)

        return table.drop(*colstodrop)
    except Exception:
        logger.log("Error while trying to remove columns with all null values","ERROR")

# COMMAND ----------

def remove_null_rows(table):
    """
    Removes rows with null values
    
    Parameters:
    ----------
    table : Pyspark Dataframe(pyspark.sql.dataframe.DataFrame)
        Name of the dataframe 
        
    Returns:
    -------
    Pyspark Dataframe(pyspark.sql.dataframe.DataFrame)
    """
    try:
        return table.dropna()
    except Exception:
        logger.log("Error while trying to remove columns with null values", "ERROR")

# COMMAND ----------

def data_cleaning_daily(spark,db_table,df,enriched_data_path,partition_col,mode):
    """
    Checks if data has been cleaned for the current day.
    If not present it writes the enriched data into delta tables.

    Parameters:
    ----------
    path_exist : bool
        Checks if path to the data is present or not

    Returns:
    -------
    None
    """
    try:
        if path_exist:
            if get_partitioned_data_from_delta(spark,enriched_data_path,date.today().strftime('%Y-%m-%d')).count() == 0:
                write_to_delta_lake(spark,db_table,df,enriched_data_path,partition_col,mode)
            else:
                print("Data cleaning has been performed for today's data")
                logger.log("Data cleaning has been performed for today's data","INFO")
        else:
            write_to_delta_lake(spark,db_table,df,enriched_data_path,partition_col,mode)
    
    except Exception:
        logger.log("Error trying to write today's data into enriched_daily","ERROR")

# COMMAND ----------

def data_cleaning_all(spark,db_table,df,enriched_data_path,partition_col,mode):
    """
    Performs data cleaning on all the data and overwrites all the tables.

    Parameters:
    ----------

    Returns:
    -------
    None
    """
    try:
        write_to_delta_lake(spark,db_table,df,enriched_data_path,partition_col,mode)
    except Exception:
        logger.log("Error trying to write all the data into enriched_daily", "ERROR")
