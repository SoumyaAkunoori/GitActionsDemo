# Data Pipeline

## Dataset
TPC-DS is a dataset used for evaluating the performance of database systems. It is based on a decision support system and includes a wide range of data such as sales data, customer data, and product information. TPC-DS is commonly used to evaluate the performance of data warehouses and other systems that process large amounts of data.

TPC-DS tool was used to generate 1GB of data. The data was loaded into SQL Database using Azure Data Factory.

## Data Pipeline Layers 

This data pipeline consists of three layers:

Raw Layer: This is the initial layer where the raw data is stored. The raw data is usually unstructured and has not been processed in any way. In this layer we are fetching the tables from SQL database into Pyspark dataframe. We are then writing it into external Delta lake tables. No preprocessing is done in this layer.

Enriched Layer: The raw data is then processed and transformed in the enriched layer to add additional context and information. In this layer we are reading the data stored in the raw layer of external delta lake tables and performing data cleaning operations such as removing duplicates, null rows and null columns. After the Data cleaning is done we are writing the data into the enriched layer of external delta lake tables. 

Curated Layer: The curated layer is the final stage of the pipeline where the data is ready for analysis and consumption. In this layer we are reading the enriched layer tables and then performing join and aggregate operations to get the final sales tables which consists of store sales and web sales information. We are then writing the data into the curated layer external tables.


Data Quality checks are also performed in each layer to check the accuracy, completeness, consistency, timeliness, validity, and uniqueness of the data. Great expectations has been used for the Data Quality checks.