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
    defaultValue="../../../../../../conf/tpc_ds/raw-dq-workload.yml",
    label="Config File Path",
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

#Load config files
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

data_context_config = DataContextConfig(
    store_backend_defaults=FilesystemStoreBackendDefaults(
        root_directory=root_directory
    ),
)
context = BaseDataContext(project_config=data_context_config)

# COMMAND ----------

logger.log("Starting DQ check for Raw Data","INFO")

# COMMAND ----------

logger.log("Reading Raw Data From ADLS","INFO")
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

store_sales_datasource_config = {
    "name": "store_sales",
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

catalog_sales_datasource_config = {
    "name": "catalog_sales",
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

web_sales_datasource_config = {
    "name": "web_sales",
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

inventory_datasource_config = {
    "name": "inventory",
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
    except AssertionError:
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
    except AssertionError:
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

# Store sales
context.test_yaml_config(yaml.dump(store_sales_datasource_config))
context.add_datasource(**store_sales_datasource_config)

batch_request = RuntimeBatchRequest(
    datasource_name="store_sales",
    data_connector_name="spark_delta_connector",
    data_asset_name="store_sales_delta",  
    batch_identifiers={
        "pipeline_stage": "prod",
        "run_id": f"my_run_name_{datetime.date.today().strftime('%Y%m%d')}",
    },
    runtime_parameters={
        "batch_data": store_sales_spark_df
    }, 
)

expectation_suite_name = "Store Sales Raw data ingestion"
context.create_expectation_suite(
    expectation_suite_name=expectation_suite_name, overwrite_existing=True
)
store_sales_validator = context.get_validator(
    batch_request=batch_request,
    expectation_suite_name=expectation_suite_name,
)

logger.log("Asserting if null values are present primary key","INFO")
dq_null_check(store_sales_validator, "store_sales")

logger.log("Asserting if composite primary keys are unique","INFO")
dq_unique_id_check(store_sales_validator, "store_sales")
dq_check_column_range(
    store_sales_validator, column="ss_promo_sk", start_range=0, end_range=1
)

logger.log("Asserting if data is of given shape","INFO")
dq_check_data_shape(
    store_sales_validator, expected_rows=2880404, expected_columns=246
)

logger.log(
    "Asserting if the column does not have null values above a given prescribed limit",
    "INFO"
)
dq_check_column_null_percentage(store_sales_validator, "ss_net_profit", 1)

store_sales_validator.save_expectation_suite(discard_failed_expectations=False)

my_checkpoint_name = "checkpoint store sales"
expectation_suite_name = "Store Sales Raw data ingestion"
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

# Catalog sales
context.test_yaml_config(yaml.dump(catalog_sales_datasource_config))
context.add_datasource(**catalog_sales_datasource_config)

batch_request = RuntimeBatchRequest(
    datasource_name="catalog_sales",
    data_connector_name="spark_delta_connector",
    data_asset_name="catalog_sales_delta", 
    batch_identifiers={
        "pipeline_stage": "prod",
        "run_id": f"my_run_name_{datetime.date.today().strftime('%Y%m%d')}",
    },
    runtime_parameters={
        "batch_data": catalog_sales_spark_df
    },  # Your dataframe goes here
)

expectation_suite_name = "Catalog Sales Raw data ingestion"
context.create_expectation_suite(
    expectation_suite_name=expectation_suite_name, overwrite_existing=True
)
catalog_sales_validator = context.get_validator(
    batch_request=batch_request,
    expectation_suite_name=expectation_suite_name,
)


dq_null_check(catalog_sales_validator, "catalog_sales")
dq_unique_id_check(catalog_sales_validator, "catalog_sales")
dq_check_column_range(
    catalog_sales_validator, column="cs_promo_sk", start_range=0, end_range=350
)
dq_check_data_shape(
    catalog_sales_validator, expected_rows=1441548, expected_columns=35
)
dq_check_column_null_percentage(catalog_sales_validator, "cs_net_profit", 0.9)

catalog_sales_validator.save_expectation_suite(
    discard_failed_expectations=False
)

my_checkpoint_name = "checkpoint store sales"
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

# web sales
context.test_yaml_config(yaml.dump(web_sales_datasource_config))
context.add_datasource(**web_sales_datasource_config)

batch_request = RuntimeBatchRequest(
    datasource_name="web_sales",
    data_connector_name="spark_delta_connector",
    data_asset_name="web_sales_delta",
    batch_identifiers={
        "pipeline_stage": "prod",
        "run_id": f"my_run_name_{datetime.date.today().strftime('%Y%m%d')}",
    },
    runtime_parameters={
        "batch_data": web_sales_spark_df
    },  # Your dataframe goes here
)

expectation_suite_name = "Web Sales Raw data ingestion"
context.create_expectation_suite(
    expectation_suite_name=expectation_suite_name, overwrite_existing=True
)
web_sales_validator = context.get_validator(
    batch_request=batch_request,
    expectation_suite_name=expectation_suite_name,
)


dq_null_check(web_sales_validator, "web_sales")
dq_unique_id_check(web_sales_validator, "web_sales")
dq_check_column_range(
    web_sales_validator, column="ws_promo_sk", start_range=0, end_range=350
)

dq_check_column_null_percentage(web_sales_validator, "ws_net_profit", 0.9)

web_sales_validator.save_expectation_suite(discard_failed_expectations=False)

my_checkpoint_name = "checkpoint store sales"
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

# inventory sales
context.test_yaml_config(yaml.dump(inventory_datasource_config))
context.add_datasource(**inventory_datasource_config)

batch_request = RuntimeBatchRequest(
    datasource_name="inventory",
    data_connector_name="spark_delta_connector",
    data_asset_name="inventory_delta",
    batch_identifiers={
        "pipeline_stage": "prod",
        "run_id": f"my_run_name_{datetime.date.today().strftime('%Y%m%d')}",
    },
    runtime_parameters={
        "batch_data": inventory_spark_df
    }, 
)

expectation_suite_name = "Inventory Raw data ingestion"
context.create_expectation_suite(
    expectation_suite_name=expectation_suite_name, overwrite_existing=True
)
inventory_validator = context.get_validator(
    batch_request=batch_request,
    expectation_suite_name=expectation_suite_name,
)


dq_null_check(inventory_validator, "inventory")
dq_unique_id_check(inventory_validator, "inventory")
dq_check_column_range(
    inventory_validator,
    column="inv_quantity_on_hand",
    start_range=0,
    end_range=350,
)
dq_check_data_shape(
    inventory_validator, expected_rows=11745000, expected_columns=5
)
dq_check_column_null_percentage(
    inventory_validator, "inv_quantity_on_hand", 0.9
)

inventory_validator.save_expectation_suite(discard_failed_expectations=False)

my_checkpoint_name = "checkpoint store sales"
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

logger.log("DQ check for Raw Data Completed","INFO")
