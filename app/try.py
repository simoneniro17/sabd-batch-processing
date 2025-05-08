from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("example") \
    .getOrCreate()

data = [("Alice", 25), ("Bob", 30), ("Cathy", 22)]
df = spark.createDataFrame(data, ["Name", "Age"])

df.show()

spark.stop()


# # Create a SparkSession
# spark = SparkSession.builder \
#     .appName("PySpark Local Test") \
#     .master("local[*]") \
#     .getOrCreate()

# # Create a simple DataFrame
# data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
# df = spark.createDataFrame(data, ["Name", "Age"])

# # Show the DataFrame
# print("DataFrame created successfully:")
# df.show()

# # Get basic statistics
# print("Basic statistics for Age column:")
# df.select("Age").describe().show()

# # Stop the SparkSession
# spark.stop()

# print("PySpark test completed successfully!")