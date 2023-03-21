# Databricks notebook source
def fetch_min_max(df):
    
    """
    Fetches minimum and maximum values of numerical columns and all distinct value of non numerical columns of the dataframe and lists out the types of all the columns present
    
    Parameters:
    ----------
    final_df : Pyspark Dataframe(pyspark.sql.dataframe.DataFrame) 
        
        
    Returns:
    -------
    min_max: Python Dictionary
        dictionary of minimum and maximum values of numerical columns and all distinct value of non numerical columns
    fin_types: Python List
        list of the types of all the columns present
    """
    try:
        df_pd = df.toPandas()
        df_num= df_pd.select_dtypes(include=np.number)
        num_cols = df_num.columns

        min_val = []
        max_val = []

        for i in num_cols:
            min_val.append(min(df_num[i]))
            max_val.append(max(df_num[i]))  

        non_num = df_pd.select_dtypes(exclude=np.number)
        non_num_cols = non_num.columns
        non_list = [list(non_num[non_num.columns[i]].unique()) for i in range(0, len(non_num_cols))]

        cols = list(num_cols) + list(non_num_cols)

        min_max_list = [(min_val[i], max_val[i]) for i in range(0, len(num_cols))]
        col_types1 = [ df.select(col).dtypes[0][1] for col in num_cols]
        col_types2 = [ df.select(col).dtypes[0][1] for col in non_num_cols]
        fin_types = col_types1+col_types2
        min_max = dict(zip(cols, min_max_list+non_list))
        logger.log("Min max values for numericals, Distinct values for categoricals generated","INFO")
        return min_max,fin_types
    
    except Exception:
        logger.log('Error in replace_cat function',"ERROR")

# COMMAND ----------

def generate_data(min_max_dict,col_types_list, df,today):
    """
    Generates synthetic data with columns in specified ranges
    
    Parameters:
    ----------
    None
        
    Returns:
    -------
    gen_data: PySpark Dataframe
        Dataframe of synthetic generated data
    """
    try:
        keys = list(min_max_dict.keys())
        testDataSpec = (dg.DataGenerator(spark, rows=10000, 
                  randomSeedMethod="hash_fieldname")
        #             .withSchema(df_schema)
                   .withColumn(keys[1], col_types_list[1], minValue=min_max_dict[keys[1]][0], maxValue=min_max_dict[keys[1]][1])
                   .withColumn(keys[2], col_types_list[2], minValue=min_max_dict[keys[2]][0], maxValue=min_max_dict[keys[2]][1])
                   .withColumn(keys[3], col_types_list[3],minValue=min_max_dict[keys[3]][0], maxValue=min_max_dict[keys[3]][1])
                   .withColumn(keys[4], col_types_list[4],minValue=min_max_dict[keys[4]][0], maxValue=min_max_dict[keys[4]][1])
                   .withColumn(keys[5], col_types_list[5],minValue=min_max_dict[keys[5]][0], maxValue=min_max_dict[keys[5]][1])
                   .withColumn(keys[6], col_types_list[6],minValue=min_max_dict[keys[6]][0], maxValue=min_max_dict[keys[6]][1])
                   .withColumn(keys[7], col_types_list[7],minValue=min_max_dict[keys[7]][0], maxValue=min_max_dict[keys[7]][1])
                   .withColumn(keys[8], col_types_list[8],minValue=min_max_dict[keys[8]][0], maxValue=min_max_dict[keys[8]][1])
                   .withColumn(keys[9], col_types_list[9],minValue=min_max_dict[keys[9]][0], maxValue=min_max_dict[keys[9]][1])
                   .withColumn(keys[10], col_types_list[10], values= min_max_dict[keys[10]])
                   .withColumn(keys[11],col_types_list[11], values=[today])
                   )
        gen_data = testDataSpec.build()
        col_list = ['house_id']
        col_list = col_list+ gen_data.columns
        gen_data = gen_data.select("*").withColumn("house_id", F.monotonically_increasing_id()+1)
        gen_data = gen_data.select(col_list)
        logger.log("Daily data generated","INFO")
        return gen_data
    
    except Exception:
        logger.log('Error in generate_data function',"ERROR")


# COMMAND ----------

def write_to_delta(spark,partition_date):
    """
    Generates data and writes it to delta tables 

    Parameters:
    ----------
    partition_date : string
        Date on which data will be generated

    Returns:
    -------
    None
    """
    final_data = generate_data(min_max_dict,col_types_list,df,partition_date)
    write_to_delta_lake(spark,f"{db}.{table}",final_data,raw_data_path,"Date","append")

# COMMAND ----------

def write_data_daily(spark,path_exist):
    """
    Checks if daily for the current day is present or not.
    If not present it calls the write_to_delta().

    Parameters:
    ----------
    path_exist : bool
        Checks if path to the data is present or not

    Returns:
    -------
    None
    """
    try:
        if path_exist:
            if get_partitioned_data_from_delta(spark,raw_data_path,date.today().strftime('%Y-%m-%d')).count() == 0:
                write_to_delta(spark,date.today().strftime('%Y-%m-%d'))
            else:
                logger.log("Data for the current day is already present","INFO")
        else:
            write_to_delta(spark,date.today().strftime('%Y-%m-%d'))
            
    except Exception:
        logger.log("Error in writing daily data to delta tables", "ERROR")

# COMMAND ----------

def write_data_date_range(spark,path_exist):
    """
    Checks if data for a date range is present or not.
    If not present it calls the write_to_delta().

    Parameters:
    ----------
    path_exist : bool
        Checks if path to the data is present or not

    Returns:
    -------
    None
    """
    try:
        date_format = "%Y-%m-%d"
        start = datetime.datetime.strptime(start_date, date_format)
        end = datetime.datetime.strptime(end_date, date_format)
        date = start
        offset = timedelta(days=1)

        while date!=end+offset:
            if path_exist:
                if get_partitioned_data_from_delta(spark,raw_data_path,date.strftime('%Y-%m-%d')).count() == 0:
                    write_to_delta(spark,date.strftime('%Y-%m-%d'))
                else:
                    logger.log("Data for the current day is already present","INFO")
            else:
                write_to_delta(spark,date.strftime('%Y-%m-%d'))

            date = date + offset
            
    except Exception as e:
        logger.log("Error in writing date range data to delta tables","ERROR")
