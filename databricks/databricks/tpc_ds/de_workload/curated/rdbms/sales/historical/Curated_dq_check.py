# Databricks notebook source
# MAGIC %run ../../../../../../io_utils

# COMMAND ----------

# MAGIC %run ../../../../../../logging_utils

# COMMAND ----------

import datetime

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
    defaultValue="../../../../../../conf/tpc_ds/curated-dq-workload.yml",
    label="DQ Config File Path",
)
dbutils.widgets.text(
    name="log_config_file_path",
    defaultValue="../../../../../../conf/logs/logger_config.yml",
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
dbutils.widgets.text(
    name="root_directory",
    defaultValue="/dbfs/great_expectations/",
    label="Root Directory",
)

# COMMAND ----------

config_file_path = dbutils.widgets.get("config_file_path")
log_config_file_path = dbutils.widgets.get("log_config_file_path")
storage_name = dbutils.widgets.get("storage_name")
scope = dbutils.widgets.get("scope")
root_directory = dbutils.widgets.get("root_directory")

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

data_context_config = DataContextConfig(
    store_backend_defaults=FilesystemStoreBackendDefaults(
        root_directory=root_directory
    ),
)
context = BaseDataContext(project_config=data_context_config)

# COMMAND ----------

# Path to curated data
input_container_path = config["adls"]["input_container_path"]
table = config["table"]

# COMMAND ----------

logger.log("Starting DQ check for Curated Data","INFO")

# COMMAND ----------

logger.log("Reading Curated Data From ADLS","INFO")
final_df = get_data_from_adls(spark, f"{input_container_path}/{table}")

# COMMAND ----------

def check_mandatory_cols(df_ge, columns):
    """
    Checks if mandatory columns are present in final_df

    Parameters:
    ----------
    df_ge: Great Expectation Dataframe
    columns: list
        List of mandatory columns

    Returns:
    -------
    None
    """
    for column in columns:
        try:
            test_result = df_ge.expect_column_to_exist(column)
            if test_result.success:
                print(f"Column {column} exists : PASSED")
            else:
                print(
                    f"Uh oh! Mandatory column {column} does not exist: FAILED"
                )

        except AssertionError:
            logger.log("AssertionError acquired while trying to do null check","ERROR")

# COMMAND ----------

def check_cols_are_num(df_ge, columns):
    """
    Checks if the columns are of numerical type

    Parameters:
    ----------
    df_ge: Great Expectation Dataframe 
    columns: list
        List of mandatory columns

    Returns:
    -------
    None
    """
    for col in columns:
        num_test_result = df_ge.expect_column_values_to_be_in_type_list(
            col, ["IntegerType", "LongType", "DecimalType", "DoubleType"]
        )
        try:
            if num_test_result.success:
                print(f" {col} is numerical: PASSED")
            else:
                f"{num_test_result.result['unexpected_count']} of" 
                f" {num_test_result.result['element_count']} items in column {column} are not numerical:  FAILED"
        except AssertionError:
            logger.log("Columns are not of numerical type","ERROR")

# COMMAND ----------

def check_date_type(df_ge, columns):
    """
    Checks if date column is of DateType

    Parameters:
    ----------
    df_ge: Great Expectation Dataframe (great_expectations.dataset.sparkdf_dataset.SparkDFDataset)

    Returns:
    -------
    None
    """
    for column in columns:
        type_test_result = df_ge.expect_column_values_to_be_in_type_list(
            column, ["DateType"]
        )
        try:
            if type_test_result.success:
                print(f"{column} column is of type - DateType: PASSED")
            else:
                print(f"{column} column is not of DateType, FAILED")
                logger.log(f"{column} column is not of DateType, FAILED","ERROR")
        except AssertionError:
            logger.log("Columns are not of expected Datatype","ERROR")

# COMMAND ----------

final_df_datasource_config = {
    "name": "final_df",
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

context.test_yaml_config(yaml.dump(final_df_datasource_config))
context.add_datasource(**final_df_datasource_config)

batch_request = RuntimeBatchRequest(
    datasource_name="final_df",
    data_connector_name="spark_delta_connector",
    data_asset_name="final_df_delta", 
    batch_identifiers={
        "pipeline_stage": "prod",
        "run_id": f"my_run_name_{datetime.date.today().strftime('%Y%m%d')}",
    },
    runtime_parameters={"batch_data": final_df},  
)

expectation_suite_name = "Curated Final DF"
context.create_expectation_suite(
    expectation_suite_name=expectation_suite_name, overwrite_existing=True
)
final_df_validator = context.get_validator(
    batch_request=batch_request,
    expectation_suite_name=expectation_suite_name,
)

check_mandatory_cols(final_df_validator, config["mandatory_cols"])
check_cols_are_num(final_df_validator, config["numerical_cols"])
check_date_type(final_df_validator, config["date_cols"])

final_df_validator.save_expectation_suite(discard_failed_expectations=False)

my_checkpoint_name = "checkpoint final df"
checkpoint_config = {
    "name": my_checkpoint_name,
    "config_version": 1.0,
    "class_name": "SimpleCheckpoint",
    "run_name_template": "%Y%m%d-%H%M%S-my-run-name-template",
}

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

logger.log("DQ check for Curated Data completed","INFO")
