# Databricks notebook source
import os.path as op

import yaml

# COMMAND ----------

# Setting up spark monitoring and logging
log4jLogger = sc._jvm.org.apache.log4j
log = log4jLogger.LogManager.getLogger(__name__)

# COMMAND ----------

dbutils.widgets.text(
    name="storage_name",
    defaultValue="mlopscompletepocsa",
    label="Storage Name",
)
dbutils.widgets.text(
    name="config_container_name",
    defaultValue="config",
    label="Config Container Name",
)
dbutils.widgets.text(
    name="data_container_name",
    defaultValue="data",
    label="Data Container Name",
)
dbutils.widgets.text(
    name="scope", defaultValue="datagen2", label="Databricks Scope"
)
dbutils.widgets.text(
    name="root_directory",
    defaultValue="/dbfs/great_expectations/",
    label="Root Directory",
)
dbutils.widgets.text(
    name="log_config_file_path",
    defaultValue="mars-mlops-poc/dev/app/logger_config.yml",
    label="Log config File Path",
)
dbutils.widgets.text(
    name="log_dbfs_file_path",
    defaultValue="mars-mlops-poc/config/logger_config.yml",
    label="Log DBFS File Path",
)

# COMMAND ----------

# storage account properties
storage_name = dbutils.widgets.get("storage_name")
config_container_name = dbutils.widgets.get("config_container_name")
data_container_name = dbutils.widgets.get("data_container_name")

# vault scope
scope = dbutils.widgets.get("scope")

# root directory to store html
root_directory = dbutils.widgets.get("root_directory")

# COMMAND ----------

# Service principal secrets
service_credential = dbutils.secrets.get(scope=f"{scope}", key="sp-secret")
application_id = dbutils.secrets.get(scope=f"{scope}", key="application-id")
directory_id = dbutils.secrets.get(scope=f"{scope}", key="directory-id")

# Connnection configuration for Datalake
spark.conf.set(
    f"fs.azure.account.auth.type.{storage_name}.dfs.core.windows.net", "OAuth"
)
spark.conf.set(
    f"fs.azure.account.oauth.provider.type.{storage_name}.dfs.core.windows.net",
    "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
)
spark.conf.set(
    f"fs.azure.account.oauth2.client.id.{storage_name}.dfs.core.windows.net",
    f"{application_id}",
)
spark.conf.set(
    f"fs.azure.account.oauth2.client.secret.{storage_name}.dfs.core.windows.net",
    service_credential,
)
spark.conf.set(
    f"fs.azure.account.oauth2.client.endpoint.{storage_name}.dfs.core.windows.net",
    f"https://login.microsoftonline.com/{directory_id}/oauth2/token",
)

# COMMAND ----------

def config_load(config_file_path, dbfs_file_path):
    """
    Copies config file present in adls to dbfs and loads it

    Parameters:
    ----------
    config_file_path : string
        path of config file present in adls
        
    dbfs_file_path : string
        target path of config in dbfs

    Returns:
    -------
    yaml file
    """
    try:
        dbutils.fs.cp(
            f"""abfss://{config_container_name}@{storage_name}"""
            f""".dfs.core.windows.net/{config_file_path}""",
            f"dbfs:/{dbfs_file_path}",
        )
        config_path = op.join("/dbfs", f"{dbfs_file_path}")
        with open(config_path, "r") as fp:
            return yaml.safe_load(fp)

    except RuntimeError:
        log.error("Error while loading conifg file from dbfs")

# COMMAND ----------

def write_to_delta_lake(table, table_df, path, partition_by_col_name = "partition_date", mode = "overwrite"):
    """
    Transfers data stored in Dataframe to Delta lake tables

    Parameters:
    ----------
    tableName : string
        Name of the table
    table : pyspark dataframe
        table dataframe
    path : string
        output path of the delta tables
    db : string
        database to store the delta tables
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
        ).option(
            "path",
            f"abfss://{data_container_name}@{storage_name}."
            f"dfs.core.windows.net/{path}/{table.split('.')[-1]}"
        ).mode(
            mode
        ).saveAsTable(
            f"{table}"
        )
    except RuntimeError:
        log.error("Error while transfering df to delta lake table")

# COMMAND ----------

def get_data_from_adls(table, path):
    """
    Reads data from delta files stored in adls into spark dataframes.

    Parameters:
    ----------
    table : String
        Name of the table
    path : String 
        Path of the delta table stored in adls

    Returns:
    -------
    Pyspark Dataframe(pyspark.sql.dataframe.DataFrame)
    """
    try:
        return (
            spark.read.option("header", "true")
            .format("delta")
            .load(
                f"abfss://{data_container_name}@{storage_name}.dfs.core.windows.net/"
                f"{path}/{table}"
            )
        )
    except RuntimeError:
        log.error("Error while loading table", table, "from adls")

# COMMAND ----------

def get_df_from_database(table, jdbcUrl):
    """
    Fetches the tables present in SQL Database and stores them in dataframe

    Parameters:
    ----------
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
        log.error(f"Error while loading table {table}")

# COMMAND ----------


