# Databricks notebook source
#imports
import pandas as pd
import numpy as np
import pytest
import ipytest
from pyspark.sql.types import StructType,StructField, StringType, IntegerType

# COMMAND ----------

# MAGIC %run ../model_monitoring/model_monitoring_func

# COMMAND ----------

ipytest.autoconfig()

# COMMAND ----------

data = [-5, 10, 6, -2]
expected_res = [0, 1, 1, 0]

# COMMAND ----------

# MAGIC %%ipytest
# MAGIC def test_check_sign():
# MAGIC     
# MAGIC     result = check_sign(data)
# MAGIC #     print(type(latest_version))
# MAGIC     expected = expected_res
# MAGIC     print(result)
# MAGIC     assert result == expected
# MAGIC     
# MAGIC test_check_sign()
