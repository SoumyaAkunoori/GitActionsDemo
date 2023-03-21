# Databricks notebook source
# MAGIC %run ../../../../../../io_utils

# COMMAND ----------

# MAGIC %run ../../../../../../logging_utils

# COMMAND ----------

from pyspark.sql.functions import lit, sum

# COMMAND ----------

dbutils.widgets.text(
    name="config_file_path",
    defaultValue="../../../../../../conf/tpc_ds/curated-de-workload.yml",
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

# COMMAND ----------

config_file_path = dbutils.widgets.get("config_file_path")
storage_name = dbutils.widgets.get("storage_name")
scope = dbutils.widgets.get("scope")
log_config_file_path = dbutils.widgets.get("log_config_file_path")

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

# Path to raw data
input_container_path = config["adls"]["input_container_path"]

# Output Path of enriched data
output_container_path = config["adls"]["output_container_path"]

# Database Tables
tables = config["tables"]

# Database to store curated table
db = config["adls"]["database"]

# COMMAND ----------

logger.log("Starting Curated pipeline","INFO")

# COMMAND ----------

# Read data from Enriched layer
logger.log("Reading Enriched Data From ADLS","INFO")
for table in tables:
    globals()[table] = get_data_from_adls(spark, f"{input_container_path}/{table}")

# COMMAND ----------

def get_store_sales():
    """
    Joins Date,Item and Store tables with Store sales and return store_sales df

    Parameters:
    ----------
    None

    Returns:
    -------
    Pyspark Dataframe(pyspark.sql.dataframe.DataFrame)
    """
    try:
        return (
            store_sales.join(
                date_dim,
                store_sales.ss_sold_date_sk == date_dim.d_date_sk,
                "inner",
            )
            .join(item, store_sales.ss_item_sk == item.i_item_sk, "inner")
            .join(store, store_sales.ss_store_sk == store.s_store_sk, "inner")
            .withColumn("source", lit("Store"))
            .withColumn("ext_ship_cost", lit(0.0))
            .select(
                store_sales["partition_date"],
                date_dim["d_date"].alias("date"),
                date_dim["d_year"].alias("year"),
                date_dim["d_quarter_name"].alias("quarter_name"),
                item["i_brand"].alias("brand"),
                item["i_manufact"].alias("manufact"),
                item["i_product_name"].alias("product_name"),
                "source",
                store["s_store_name"].alias("store_name"),
                store_sales["ss_quantity"].alias("sold_quantity"),
                store_sales["ss_ext_wholesale_cost"].alias(
                    "ext_wholesale_cost"
                ),
                store_sales["ss_ext_list_price"].alias("ext_list_price"),
                "ext_ship_cost",
                store_sales["ss_ext_tax"].alias("ext_tax"),
                store_sales["ss_coupon_amt"].alias("coupon_amt"),
                store_sales["ss_net_paid"].alias("net_paid"),
                store_sales["ss_net_paid_inc_tax"].alias("net_paid_inc_tax"),
            )
        )
    except ValueError:
        logger.log("Error while trying to join dataframes","ERROR")

# COMMAND ----------

def get_web_sales():
    """
    Joins Date,Item and Web_site tables with web_sales and return web_sales df

    Parameters:
    ----------
    None

    Returns:
    -------
    Pyspark Dataframe(pyspark.sql.dataframe.DataFrame)
    """
    try:
        return (
            web_sales.join(
                date_dim,
                web_sales.ws_sold_date_sk == date_dim.d_date_sk,
                "inner",
            )
            .join(item, web_sales.ws_item_sk == item.i_item_sk, "inner")
            .join(
                web_site,
                web_sales.ws_web_site_sk == web_site.web_site_sk,
                "inner",
            )
            .withColumn("source", lit("Web"))
            .select(
                web_sales["partition_date"],
                date_dim["d_date"].alias("date"),
                date_dim["d_year"].alias("year"),
                date_dim["d_quarter_name"].alias("quarter_name"),
                item["i_brand"].alias("brand"),
                item["i_manufact"].alias("manufact"),
                item["i_product_name"].alias("product_name"),
                "source",
                web_site["web_name"].alias("store_name"),
                web_sales["ws_quantity"].alias("sold_quantity"),
                web_sales["ws_ext_wholesale_cost"].alias("ext_wholesale_cost"),
                web_sales["ws_ext_list_price"].alias("ext_list_price"),
                web_sales["ws_ext_ship_cost"].alias("ext_ship_cost"),
                web_sales["ws_ext_tax"].alias("ext_tax"),
                web_sales["ws_coupon_amt"].alias("coupon_amt"),
                web_sales["ws_net_paid"].alias("net_paid"),
                web_sales["ws_net_paid_inc_tax"].alias("net_paid_inc_tax"),
            )
        )
    except ValueError:
        logger.log("Error while trying to join dataframes","ERROR")

# COMMAND ----------

def concat_sales(store_sales_df, web_sales_df):
    """
    Concatenates the store_sales and web_sales dataframe

    Parameters:
    ----------
    store_sales_df: Pyspark Dataframe(pyspark.sql.dataframe.DataFrame)
    web_sales_df: Pyspark Dataframe(pyspark.sql.dataframe.DataFrame)

    Returns:
    -------
    Pyspark Dataframe(pyspark.sql.dataframe.DataFrame)
    """
    try:
        return store_sales_df.union(web_sales_df)
    except ValueError:
        logger.log(
            "Error while trying to concatenate the store_sales and web_sales dataframe",
            "ERROR"
        )

# COMMAND ----------

def sales_agg(sales_df):
    """
    Performs aggregation on the sales dataframe

    Parameters:
    ----------
    sales_df: Pyspark Dataframe(pyspark.sql.dataframe.DataFrame)

    Returns:
    -------
    Pyspark Dataframe(pyspark.sql.dataframe.DataFrame)
    """
    try:
        return sales_df.groupBy(
            "partition_date",
            "date",
            "year",
            "quarter_name",
            "brand",
            "manufact",
            "product_name",
            "source",
            "store_name",
        ).agg(
            sum("sold_quantity").alias("total_sold_quantity"),
            sum("ext_wholesale_cost").alias("total_ext_wholesale_cost"),
            sum("ext_list_price").alias("total_ext_list_price"),
            sum("ext_ship_cost").alias("total_ext_ship_cost"),
            sum("ext_tax").alias("total_ext_tax"),
            sum("coupon_amt").alias("total_coupon_amt"),
            sum("net_paid").alias("total_net_paid"),
            sum("net_paid_inc_tax").alias("total_net_paid_inc_tax"),
        )
    except ValueError:
        logger.log(
            "Error while trying to perform aggregation on the sales dataframe",
            "ERROR"
        )

# COMMAND ----------

# Read store sales and web sales data
store_sales_df = get_store_sales()
web_sales_df = get_web_sales()

# COMMAND ----------

# Concatenate store sales and web sales
sales_df = concat_sales(store_sales_df, web_sales_df)

# COMMAND ----------

# Sales aggregation
final_df = sales_agg(sales_df)

# COMMAND ----------

# Write data into datalake
logger.log("Writing Data to Datalake Curated layer","INFO")
write_to_delta_lake(spark, f"{db}.final_df", final_df, f"{output_container_path}/final_df")
logger.log("Multiple data sources detected for transfer","WARN")

# COMMAND ----------

logger.log("Curated pipeline Completed","INFO")
