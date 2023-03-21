# Databricks notebook source
import os
import os.path as op
from pyspark.sql.functions import when,isnull,col,count
import yaml

# COMMAND ----------

dbutils.widgets.text(name='storage_name',defaultValue='mlopscompletepocsa',label='Storage Name')
dbutils.widgets.text(name='config_container_name',defaultValue='config',label='Config Container Name')
dbutils.widgets.text(name='config_file_path',defaultValue='mars-mlops-poc/dev/app/housing_config.yml',label='Config File Path')
dbutils.widgets.text(name='dbfs_file_path',defaultValue='mars-mlops-poc/config/housing_config.yml',label='DBFS File Path')
dbutils.widgets.text(name='scope',defaultValue='datagen2',label='Databricks Scope')

# COMMAND ----------

#storage account properties
storage_name = dbutils.widgets.get('storage_name')
config_container_name = dbutils.widgets.get('config_container_name')
config_file_path = dbutils.widgets.get('config_file_path')
dbfs_file_path = dbutils.widgets.get('dbfs_file_path')

#vault scope
scope = dbutils.widgets.get('scope')

# COMMAND ----------

#Service principal secrets
service_credential = dbutils.secrets.get(scope=f"{scope}",key="sp-secret")
application_id = dbutils.secrets.get(scope=f"{scope}",key="application-id")
directory_id = dbutils.secrets.get(scope=f"{scope}",key="directory-id")

#Connnection configuration for Datalake
spark.conf.set(f"fs.azure.account.auth.type.{storage_name}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_name}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_name}.dfs.core.windows.net",f"{application_id}")
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_name}.dfs.core.windows.net", service_credential)
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_name}.dfs.core.windows.net", f"https://login.microsoftonline.com/{directory_id}/oauth2/token")

# COMMAND ----------

#copy config.yml from storage account to dbfs
dbutils.fs.cp(f"abfss://{config_container_name}@{storage_name}.dfs.core.windows.net/{config_file_path}", f"dbfs:/{dbfs_file_path}")

# COMMAND ----------

#load config.yml from dbfs
config_path = op.join("/dbfs", f"{dbfs_file_path}")
with open(config_path, 'r') as fp:
    app_config = yaml.safe_load(fp)

# COMMAND ----------

data_container_name = app_config['data_container_name']

# COMMAND ----------

def get_data_from_adls(path):
    """
    Reads data from csv files stored in adls into spark dataframes.
    
    Parameters:
    ----------
    path : String 
        path to data 
        
    Returns:
    -------
    Pyspark Dataframe(pyspark.sql.dataframe.DataFrame)
    """
    return spark.read.option("header", "true").csv(
        f"abfss://{data_container_name}@{storage_name}.dfs.core.windows.net/{path}", inferSchema=True
    )

# COMMAND ----------

def df_to_delta_table(df,path,database,table):
    """
    Transfers data stored in Pyspark Dataframe to External Delta tables
    
    Parameters:
    ----------
    df: PySpark dataframe
        df to be written to delta table
        
    path: String
        Path to store the external delta tables
        
    database: String
        database name
        
    table : string
        table name
        
    Returns:
    -------
    None
    """
    df.write.format("delta").partitionBy("Date").mode("append")\
    .option('path',f"abfss://{data_container_name}@{storage_name}.dfs.core.windows.net/{path}")\
    .saveAsTable(f"{database}.{table}")
        

# COMMAND ----------

def df_to_delta_table_all(df,path,database,table):
    """
    Transfers all data stored in Pyspark Dataframe to External Delta tables
    
    Parameters:
    ----------
    df: PySpark dataframe
        df to be written to delta table
        
    path: String
        Path to store the external delta tables
        
    database: String
        database name
        
    table : string
        table name
        
    Returns:
    -------
    None
    """
    df.write.format("delta").partitionBy("Date").mode("overwrite")\
    .option('path',f"abfss://{data_container_name}@{storage_name}.dfs.core.windows.net/{path}")\
    .saveAsTable(f"{database}.{table}")
        

# COMMAND ----------

def get_data_from_delta_all(path):
    """
    Reads all data from external delta tables stored in adls into spark dataframes.
    
    Parameters:
    ----------
    path : String 
        Path to the external delta table
        
    Returns:
    -------
    Pyspark Dataframe(pyspark.sql.dataframe.DataFrame)
    """
    return spark.read.option("header", "true").format("delta").load(
        f"abfss://{data_container_name}@{storage_name}.dfs.core.windows.net/{path}")

# COMMAND ----------

def get_data_from_delta(path,partition):
    """
    Reads data from external delta tables stored in adls into spark dataframes.
    
    Parameters:
    ----------
    path : String 
        Path to the external delta table
        
    Partition: String
        Partition date
        
    Returns:
    -------
    Pyspark Dataframe(pyspark.sql.dataframe.DataFrame)
    """
    return spark.read.option("header", "true").format("delta").load(
        f"abfss://{data_container_name}@{storage_name}.dfs.core.windows.net/{path}").where(f"Date='{partition}'")
