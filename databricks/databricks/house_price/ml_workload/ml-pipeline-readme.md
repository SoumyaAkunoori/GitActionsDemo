# ML Pipeline

## Dataset

The data contains information from the 1990 California census.
The data pertains to the houses found in a given California district and some summary stats about them based on the 1990 census data.

## ML Pipeline Layers

This data pipeline consists of two layers:

Enriched Layer: We are reading enriched daily data from this layer, which was stored after processing in Data Pipeline. After this we generate features for training and store it in delta tables in the Enriched layer. After that, we read the features and create data for training and store that in the enriched layer as well. Features are also stored in databricks feature store, and models are logged in Mlflow.


Curated Layer: After model training, scoring is carried out and the scoring results are stored in a delta table in the curated layer.

Notebooks in each layer:

Enriched:
1. 01_feature_generation_nb
2. 02_data_quality_checks
3. 03_feature_storing
4. 01_create_training_data
5. 02_training

Curated:
1. 01_scoring

