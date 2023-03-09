from pyspark.sql import SparkSession

# create a SparkSession
spark = SparkSession.builder.appName("MyApp").getOrCreate()

# create a sample data
data = [("Alice", 25), ("Bob", 30), ("Charlie", 35), ("Dave", 40)]

# create a DataFrame
df = spark.createDataFrame(data, ["name", "age"])

# show the DataFrame
df.show()
