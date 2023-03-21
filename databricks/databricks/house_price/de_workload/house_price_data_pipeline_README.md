# House Price Data Pipeline

## Dataset

The data contains information from the 1990 California census.
The data pertains to the houses found in a given California district and some summary stats about them based on the 1990 census data.

## Data Pipeline Layers

This data pipeline consists of two layers:

Raw Layer: This is the initial layer where the raw data is stored.We are using dbldatagen library to generate the synthetic data for present day or in a given date range.

Enriched Layer: The raw data is then processed and transformed in the enriched layer to add additional context and information. In this layer we are reading the data stored in the raw layer of external delta lake tables and performing data cleaning operations such as removing duplicates, null rows and null columns. After the Data cleaning is done we are writing the data into the enriched layer of external delta lake tables.

There are two notebooks in the enriched layer:

1. Enriched_nb: for preprocessing the raw data and storing it in enriched alyer
2. Enriched_dq_check: doing quality checks on enriched data.
