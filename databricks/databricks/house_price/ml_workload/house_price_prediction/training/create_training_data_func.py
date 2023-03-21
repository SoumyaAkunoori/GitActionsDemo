# Databricks notebook source
def fetch_historical_data(historical_data_format, data_path,primary_key,id_col,target_col,spark):
    """
    fetches historical data and filters required columns

    Parameters:
    ------------
      None

    Returns:
    ------------
      df: Pyspark Dataframe
        filtered historical data
    """
    df = get_data_from_adls(spark,hist_data_path , historical_data_format)
    df = req_cols(
        df,
        primary_key,
        id_col,
        target_col,
    )
    return df

# COMMAND ----------

def get_data_in_range(start_date, end_date, daily_enriched_data,primary_key,id_col,target_col,spark):
    """
    fetches data in given range and filters required columns

    Parameters:
    ------------
      start_date: string
        start date in range
      end_date: string
        end date in range
      daily_enriched_data: string
        path to enriched daily data

    Returns:
    ------------
      daily_df: Pyspark Dataframe
        data in given date range with required columns
    """
    daily_df = get_data_from_adls(spark, daily_enriched_data )
    daily_df = daily_df.filter(
        (daily_df.Date >= start_date) & (daily_df.Date <= end_date)
    )
    daily_df = req_cols(
        daily_df,
        primary_key,
        id_col,
        target_col,
    )
    return daily_df

# COMMAND ----------

def retraining_data(spark,daily_enriched_data, historical_data_format, hist_data_path,primary_key,id_col,date_col,target_col,table_name,training_path,Mode,start_date,end_date):
    """
    creates retraining data as per selected option:
    all, in given range or historical and writes it to training data table

    Parameters:
    ------------
      daily_enriched_data: string
        path to daily enriched data

    Returns:
    ------------
      None
    """

   

    if Mode == "historical":
        df = fetch_historical_data(historical_data_format,hist_data_path,primary_key,id_col,target_col,spark)
        print(df.count())

    elif Mode == "in given range":
        df = fetch_historical_data(historical_data_format,hist_data_path,primary_key,id_col,target_col,spark)
        daily_df = get_data_in_range(start_date, end_date, daily_enriched_data,primary_key,id_col,target_col,spark)
        df = df.union(daily_df)

    else:
        df = fetch_historical_data(historical_data_format, hist_data_path,primary_key,id_col,target_col,spark)
        daily_df = get_data_from_adls(spark, daily_enriched_data)
        daily_df = df = req_cols(
            daily_df,
            primary_key,
            id_col,
            target_col,
        )
        df = df.union(daily_df)
        print(df.count)

    write_to_delta_lake(
        spark, table_name, df, training_path, date_col, mode="overwrite"
    )
