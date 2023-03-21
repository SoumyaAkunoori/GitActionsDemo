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
    defaultValue="../../../../../../conf/tpc_ds/enriched-dq-workload.yml",
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

# Path to curated data
input_container_path = config[1]["adls"]["input_container_path"]

tables = config[2]["tables"]

# COMMAND ----------

web_returns_datasource_config = {
    "name": "web_returns",
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

catalog_returns_datasource_config = {
    "name": "catalog_returns",
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

store_returns_datasource_config = {
    "name": "store_returns",
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

# COMMAND ----------

data_context_config = DataContextConfig(
    store_backend_defaults=FilesystemStoreBackendDefaults(
        root_directory=root_directory
    ),
)
context = BaseDataContext(project_config=data_context_config)

# COMMAND ----------

logger.log("Starting DQ check for Enriched Data","INFO")

# COMMAND ----------

# Read data from Enriched layer
logger.log("Reading Enriched Data From ADLS","INFO")
for table in tables:
    globals()[table] = get_data_from_adls(spark, f"{input_container_path}/{table}")

# COMMAND ----------

store_sales_spark_df = globals()["store_sales"]
catalog_sales_spark_df = globals()["catalog_sales"]
web_sales_spark_df = globals()["web_sales"]

inventory_spark_df = globals()["inventory"]

web_returns_spark_df = globals()["web_returns"]
catalog_returns_spark_df = globals()["catalog_returns"]
store_returns_spark_df = globals()["store_returns"]

# COMMAND ----------

def dq_null_check(raw_df, table_name):
    """
    Checks for null values in primary key column

    Parameters:
    ----------
    raw_df : spark dataframe
        dataframe containing data
    table_name : string
        table name

    Returns:
    -------
    None
    """

    columns = config[0]["primary_key"][table_name]

    for column in columns:
        try:
            test_result = raw_df.expect_column_values_to_not_be_null(column)
            if test_result.success:
                print(f"All items in column {column} are not null: PASSED")
            else:
                failed_msg = f"Uh oh! {test_result.result['unexpected_count']} "
                f"of {test_result.result['element_count']} items in column {column} are null: FAILED"
                print(failed_msg)
                logger.log(failed_msg,"ERROR")
        except AssertionError:
            logger.log("AssertionError acquired while trying to do null check","ERROR")

# COMMAND ----------

def dq_unique_id_check(raw_df, table_name):
    """
    Checks if composite primary keys are unique.

    Parameters:
    ----------
    raw_df : spark dataframe
        dataframe containing data.
    table_name : string
        table name

    Returns:
    -------
    None
    """
    try:
        columns = config[0]["primary_key"][table_name]

        test_result = raw_df.expect_compound_columns_to_be_unique(columns)
        failed_msg = " ".join(
            [
                f"""Uh oh!""",
                f"""{test_result.result['unexpected_count']} of {test_result.result['element_count']} items""",
                f"""or {round(test_result.result['unexpected_percent'],2)}% are not unique: FAILED""",
            ]
        )
        print(
            f"""{f'{table} has unique primary key: PASSED' if test_result.success else failed_msg}"""
        )
        if not test_result.success:
            logger.log(failed_msg,"WARN")
    except AssertionError:
        logger.log(
            "Error seen while trying to check if composite primary_keys are unqiue",
            "ERROR"
        )

# COMMAND ----------

def dq_check_column_range(raw_df, column, start_range, end_range):
    """
    Checks if the given column in the table is within range

    Parameters:
    ----------
    column : string
        Name of the column to be checked
    start_range : int
        Minimum value the column can hold
    end_range : int
        Maximum value the column can hold

    Returns:
    -------
    None
    """
    try:
        column_range_result = raw_df.expect_column_value_lengths_to_be_between(
            column=column, min_value=start_range, max_value=end_range
        )

        if column_range_result.success:
            print(f"Column {column} is between the expected range: PASSED")
        else:
            print(f"Column {column} is not between the expected range, FAILED")
            logger.log(
                f"Column {column} is not between the expected range, FAILED",
                "WARN"
            )
    except Exception as e:
        print(
            "Exception occured when trying to see if a given column is within range"
        )
        logger.log(
            "Error seen while trying to a column is within a given range",
            "ERROR"
        )

# COMMAND ----------

def dq_check_data_shape(raw_df, expected_rows, expected_columns):
    """
    Checks if the data is of given shape rows x columns

    Parameters:
    ----------
    expected_rows : int
        Expected number of rows for the data
    expected_columns : int
        Expected number of columns for the data

    Returns:
    -------
    None
    """
    try:
        row_count_result = raw_df.expect_table_row_count_to_equal(
            value=expected_rows
        )
        columns_count_result = raw_df.expect_table_column_count_to_equal(
            value=expected_columns
        )

        if row_count_result.success and columns_count_result.success:
            print(f"Data shape is as expected: PASSED")
        else:
            print(f"Data shape is not the same as expected, FAILED")
            logger.log(f"Data shape is not the same as expected, FAILED","WARN")
    except Exception as e:
        print("Error seen while trying to check if data is of given shape ", e)
        logger.log("Error seen while trying to check if data is of given shape","ERROR")

# COMMAND ----------

def dq_check_column_null_percentage(raw_df, column, null_value):
    """
    Checks if the column does not have null values above a given prescribed limit

    Parameters:
    ----------
    column : string
        Name of the column to be checked
    null_value : float
        Should not be null more than "null_value" times
        0 : Implies all to be Null
        1 : Implies none to be Null

    Returns:
    -------
    None
    """
    try:
        column_null_check = raw_df.expect_column_values_to_not_be_null(
            column, mostly=null_value
        )

        if column_null_check.success:
            print(
                f"Column does not have null values above the prescribed limit: PASSED"
            )
        else:
            logger.log(
                f"Error seen while trying to check if column" 
                f" does not have null values above a given prescribed limit",
                "WARN"
            )
    except AssertionError:
        logger.log(
            f"Error seen while trying to check" 
            f" if column does not have null values above a given prescribed limit",
            "ERROR"
        )

# COMMAND ----------

# Web returns
context.test_yaml_config(yaml.dump(web_returns_datasource_config))
context.add_datasource(**web_returns_datasource_config)

batch_request = RuntimeBatchRequest(
    datasource_name="web_returns",
    data_connector_name="spark_delta_connector",
    data_asset_name="web_returns_delta",  
    batch_identifiers={
        "pipeline_stage": "prod",
        "run_id": f"my_run_name_{datetime.date.today().strftime('%Y%m%d')}",
    },
    runtime_parameters={
        "batch_data": web_returns_spark_df
    },  # Your dataframe goes here
)

expectation_suite_name = "Web Returns Data Cleaning"
context.create_expectation_suite(
    expectation_suite_name=expectation_suite_name, overwrite_existing=True
)
web_returns_validator = context.get_validator(
    batch_request=batch_request,
    expectation_suite_name=expectation_suite_name,
)

dq_null_check(web_returns_validator, "web_returns")
dq_unique_id_check(web_returns_validator, "web_returns")
dq_check_column_range(
    web_returns_validator, column="wr_item_sk", start_range=0, end_range=0
)
dq_check_data_shape(
    web_returns_validator, expected_rows=653844, expected_columns=25
)
dq_check_column_null_percentage(web_returns_validator, "wr_order_number", 1)

web_returns_validator.save_expectation_suite(discard_failed_expectations=False)

my_checkpoint_name = "checkpoint Web returns"

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

# Catalog returns
context.test_yaml_config(yaml.dump(catalog_returns_datasource_config))
context.add_datasource(**catalog_returns_datasource_config)

batch_request = RuntimeBatchRequest(
    datasource_name="catalog_returns",
    data_connector_name="spark_delta_connector",
    data_asset_name="catalog_returns_delta", 
    batch_identifiers={
        "pipeline_stage": "prod",
        "run_id": f"my_run_name_{datetime.date.today().strftime('%Y%m%d')}",
    },
    runtime_parameters={
        "batch_data": catalog_returns_spark_df
    }, 
)

expectation_suite_name = "Catalog Returns Data Cleaning"
context.create_expectation_suite(
    expectation_suite_name=expectation_suite_name, overwrite_existing=True
)
catalog_returns_validator = context.get_validator(
    batch_request=batch_request,
    expectation_suite_name=expectation_suite_name,
)

dq_null_check(catalog_returns_validator, "catalog_returns")
dq_unique_id_check(catalog_returns_validator, "catalog_returns")
dq_check_column_range(
    catalog_returns_validator,
    column="cr_item_sk",
    start_range=0,
    end_range=3503,
)
dq_check_column_null_percentage(
    catalog_returns_validator, "cr_order_number", 0.9
)

catalog_returns_validator.save_expectation_suite(
    discard_failed_expectations=False
)

my_checkpoint_name = "checkpoint catalog returns"

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

logger.log("DQ check for Enriched Data Completed","INFO")
