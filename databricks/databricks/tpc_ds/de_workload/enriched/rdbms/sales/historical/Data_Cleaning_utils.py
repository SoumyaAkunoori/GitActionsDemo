# Databricks notebook source
from pyspark.sql.functions import col, count, isnull, when

# COMMAND ----------

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
    except RuntimeError:
        logger.log(f"Error while loading data from {table} table", "ERROR")
    except Exception:
        logger.log(f"Error while loading data from {table} table", "ERROR")
        raise

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
    
    except RuntimeError:
        logger.log("Error while trying to remove columns with all null values","ERROR")
        
    except ValueError:
        logger.log("Error while trying to remove columns with all null values","ERROR")
        raise
        
    except AttributeError:
        logger.log("Error while trying to remove columns with all null values","ERROR")
        raise
    

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
    except RuntimeError:
        logger.log("Error while trying to remove columns with null values","ERROR")
