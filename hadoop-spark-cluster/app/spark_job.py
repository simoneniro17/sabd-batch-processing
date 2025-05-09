from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("RowCounter") \
    .getOrCreate()

# Leggi da HDFS
df = spark.read.text("hdfs://namenode:9000/input/input1.txt")

# Conta le righe
row_count = df.count()

print(f"Numero di righe nel file: {row_count}")

spark.stop()
