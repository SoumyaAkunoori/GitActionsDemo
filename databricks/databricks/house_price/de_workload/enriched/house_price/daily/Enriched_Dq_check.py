# Databricks notebook source
# MAGIC %run ../../../../../io_utils

# COMMAND ----------

# MAGIC %run ../../../../../logging_utils

# COMMAND ----------

# MAGIC %run ./enriched_dq_check_func

# COMMAND ----------

import os
import datetime
from datetime import date,timedelta
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.types.base import (
    DataContextConfig,
    FilesystemStoreBackendDefaults,
)
from ruamel import yaml

# COMMAND ----------

dbutils.widgets.text(
    name="config_file_path",
    defaultValue="../../../../../conf/house_price/housing_config.yml",
    label="Config File Path",
)
dbutils.widgets.text(
    name="log_config_file_path",
    defaultValue="../../../../../conf/logs/logger_config.yml",
    label="Log config File Path",
)
dbutils.widgets.text(
    name="storage_name",
    defaultValue="mlopscompletepocsa",
    label="Storage Name",
)
dbutils.widgets.text(
    name="scope", defaultValue="datagen2", label="Databricks Scope"
)

# COMMAND ----------

config_file_path = dbutils.widgets.get("config_file_path")
log_config_file_path = dbutils.widgets.get("log_config_file_path")
storage_name = dbutils.widgets.get("storage_name")
scope = dbutils.widgets.get("scope")

# COMMAND ----------

service_credential = dbutils.secrets.get(scope=f"{scope}", key="sp-secret")
application_id = dbutils.secrets.get(scope=f"{scope}", key="application-id")
directory_id = dbutils.secrets.get(scope=f"{scope}", key="directory-id")

# COMMAND ----------

conf_dict = {
    f"fs.azure.account.auth.type.{storage_name}.dfs.core.windows.net":"OAuth",
    f"fs.azure.account.oauth.provider.type.{storage_name}.dfs.core.windows.net":
    "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    f"fs.azure.account.oauth2.client.id.{storage_name}.dfs.core.windows.net":application_id,
    f"fs.azure.account.oauth2.client.secret.{storage_name}.dfs.core.windows.net":service_credential,
    f"fs.azure.account.oauth2.client.endpoint.{storage_name}.dfs.core.windows.net":
    f"https://login.microsoftonline.com/{directory_id}/oauth2/token",
}

# COMMAND ----------

spark = set_spark_conf(spark,conf_dict)

# COMMAND ----------

# load config files
config = config_load(config_file_path)
log_config = config_load(log_config_file_path)

# COMMAND ----------

#create logger object
logger = Logger(log_config)

# COMMAND ----------

#path = app_config['out_path']
enriched_data_path = config['enriched_data_path']

# COMMAND ----------

logger.log("Starting DQ Check on Enriched Data","INFO")

# COMMAND ----------

def df_to_ge_df(df):
    try:
        df_cols = df.columns
        df_ge = ge.dataset.SparkDFDataset(df)
        return df_ge, df_cols
    except Exception as e:
        logger.log("Error converting df into ge dataframe", "ERROR")

# COMMAND ----------

house_price_spark_df = get_data_from_adls(spark, enriched_data_path)

# COMMAND ----------

df_cols = house_price_spark_df.columns
primary_cols = config["primary_key"]
num_cols = []
for col in df_cols:
    if col not in ('Date', 'ocean_proximity'):
        num_cols.append(col)

# COMMAND ----------

data_context_config = DataContextConfig(
    store_backend_defaults=FilesystemStoreBackendDefaults(
        root_directory='/dbfs/great_expectations/'
    ),
)
context = BaseDataContext(project_config=data_context_config)

house_price_datasource_config = {
    "name": "house_price",
    "class_name": "Datasource",
    "execution_engine": {"class_name": "SparkDFExecutionEngine"},
    "data_connectors": {
        "spark_delta_connector": {
            "module_name": "great_expectations.datasource.data_connector",
            "class_name": "RuntimeDataConnector",
            "batch_identifiers": [
                "pipeline_stage",
                "run_id",
            ],
        }
    },
}

# Store sales
context.test_yaml_config(yaml.dump(house_price_datasource_config))
context.add_datasource(**house_price_datasource_config)

batch_request = RuntimeBatchRequest(
    datasource_name="house_price",
    data_connector_name="spark_delta_connector",
    data_asset_name="house_price_delta",  
    batch_identifiers={
        "pipeline_stage": "prod",
        "run_id": f"my_run_name_{datetime.date.today().strftime('%Y%m%d')}",
    },
    runtime_parameters={
        "batch_data": house_price_spark_df
    }, 
)

expectation_suite_name = "House Price Data Cleaning"
context.create_expectation_suite(
    expectation_suite_name=expectation_suite_name, overwrite_existing=True
)
house_price_validator = context.get_validator(
    batch_request=batch_request,
    expectation_suite_name=expectation_suite_name,
)

logger.log("Asserting if null values are present primary key","INFO")
dq_null_check(house_price_validator,primary_cols)



logger.log("Asserting if numerical cols are of numeric type","INFO")
check_cols_are_num(house_price_validator, num_cols)

logger.log("Asserting if date column is of datetype","INFO")
check_date_type(house_price_validator)

house_price_validator.save_expectation_suite(discard_failed_expectations=False)

my_checkpoint_name = "checkpoint house price"
expectation_suite_name = "House Price Data Cleaning"
checkpoint_config = {
    "name": my_checkpoint_name,
    "config_version": 1.0,
    "class_name": "SimpleCheckpoint",
    "run_name_template": "%Y%m%d-%H%M%S-my-run-name-template",
}
    # my_checkpoint = context.test_yaml_config(yaml.dump(checkpoint_config))
context.add_checkpoint(**checkpoint_config)

checkpoint_result = context.run_checkpoint(
    checkpoint_name=my_checkpoint_name,
    validations=[
        {
            "batch_request": batch_request,
            "expectation_suite_name": expectation_suite_name,
        }
    ],
)

context.build_data_docs()

# COMMAND ----------

# df_ge, df_cols = df_to_ge_df(house_price)

# COMMAND ----------

logger.log("DQ Check on Enriched Data done...","INFO")
