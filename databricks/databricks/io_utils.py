# Databricks notebook source
import os.path as op
import os

import yaml

# COMMAND ----------

def set_spark_conf(spark, conf_dict):
    """
    sets spark.conf to connect to adls

    Parameters:
    ----------
    spark : SparkSession object
    conf_dict : dictionary
        spark.conf configurations

    Returns:
    -------
    SparkSession object
    """
    try:
        for key, value in conf_dict.items():
            spark.conf.set(key, value)

        return spark

    except RuntimeError:
        logger.log("Error setting spark configuration", "ERROR")

# COMMAND ----------

def config_load(config_path):
    """
    Copies config file present in adls to dbfs and loads it

    Parameters:
    ----------
    config_file_path : string
        path of config file present in adls

    Returns:
    -------
    yaml file
    """
    try:
        with open(config_path, "r") as fp:
            return yaml.safe_load(fp)

    except RuntimeError:
        logger.log("Error while loading conifg file from dbfs", "ERROR")
    
    except FileNotFoundError:
        # logger.log("Config file not found", "ERROR")
        raise

# COMMAND ----------

def get_df_from_database(spark, table, jdbcUrl):
    """
    Fetches the tables present in SQL Database and stores them in dataframe

    Parameters:
    ----------
    spark : SparkSession object
    table : String
        Name of the table present in SQL Database
    jdbcUrl : String
        jdbcurl to connect to azure sql database

    Returns:
    -------
    Pyspark Dataframe(pyspark.sql.dataframe.DataFrame)
    """
    try:
        return (
            spark.read.format("jdbc")
            .option("url", jdbcUrl)
            .option("header", "true")
            .option("dbtable", f"dbo.{table}")
            .load()
        )
    except RuntimeError:
        logger.log(f"Error while loading table {table}", "ERROR")
    except Exception:
        return None

# COMMAND ----------

def write_to_delta_lake(
    spark,
    table,
    table_df,
    path,
    partition_by_col_name="partition_date",
    mode="overwrite",
):
    """
    Transfers data stored in Dataframe to Delta lake tables

    Parameters:
    ----------
    spark : SparkSession object
    table : String
        Database.tableName
    table_df : pyspark dataframe
        table dataframe
    path : string
        output path of the delta tables
    partition_by_col_name : String
        default partition column
        default value = partition_date
    mode: String
        Mode to write the data into delta tables
        default value = overwrite

    Returns:
    -------
    None
    """
    try:
        table_df.write.option("header", "true").format("delta").partitionBy(
            partition_by_col_name
        ).option("path", f"{path}").mode(mode).saveAsTable(f"{table}")
    except RuntimeError:
        logger.log("Error while transfering df to delta lake table", "ERROR")

# COMMAND ----------

def get_data_from_adls(spark, path, format_="delta"):
    """
    Reads data from delta files stored in adls into spark dataframes.

    Parameters:
    ----------
    spark : SparkSession object
    path : String
        Path of the delta table stored in adls
    format : String
        Format of the file

    Returns:
    -------
    Pyspark Dataframe(pyspark.sql.dataframe.DataFrame)
    """
    try:
        return (
            spark.read.option("header", "true")
            .option("inferSchema", "true")
            .format(format_)
            .load(f"{path}")
        )
    except RuntimeError:
        logger.log("Error while reading table from adls", "ERROR")
    except Exception:
          raise


# COMMAND ----------

def get_partitioned_data_from_delta(spark, path, partition, partition_col="Date"):
    """
    Reads single day data from external delta tables stored in adls into spark dataframes.

    Parameters:
    ----------
    spark : SparkSession object
    path : String
        Path to the external delta table
    partition: String
        Partition date
    partition_col: String
        Partition col name

    Returns:
    -------
    Pyspark Dataframe(pyspark.sql.dataframe.DataFrame)
    """
    try:
        return (
            spark.read.option("header", "true")
            .format("delta")
            .load(path)
            .where(f"{partition_col}='{partition}'")
        )

    except RuntimeError:
        logger.log("Error while reading table from adls", "ERROR")
