from pyspark.sql import SparkSession
from pyspark.sql.functions import year, avg, min, max, to_timestamp, col, lit

# *   Aggregare i dati su base annua (yearly basis). Calcolare la media (average), il minimo (minimum)
#     e il massimo (maximum) dell'**intensità di carbonio** (diretta) e della **percentuale di energia a zero emissioni (CFE%)**
#     per ciascun anno dal 2021 al 2024.

# *   Utilizzando **solo i valori medi** (average) delle due metriche (intensità di carbonio e CFE%) aggregati su base annua,
#     generare **due grafici** per confrontare visivamente l'andamento (trend) per Italia e Svezia.
# ### Esempio di output
# ```
# Esempio di output:
# # date, country, carbon-mean, carbon-min, carbon-max, cfe-mean, cfe-min, cfe-max
# 2021, IT, 280.08, 121.24, 439.06, 46.305932, 15.41, 77.02
# 2022, IT, 321.617976, 121.38, 447.33, 41.244127, 13.93, 77.44
# ...
# 2021, SE, 5.946325, 1.50, 55.07, 98.962411, 92.80, 99.65
# 2022, SE, 3.875823, 0.54, 50.58, 99.551723, 94.16, 99.97
# ```

# 1. Avviare Spark Session
spark = SparkSession.builder \
    .appName("ElectricityMaps-Batch-Processing") \
    .getOrCreate()

# 2. Caricare i dati da HDFS
path_to_hdfs_file = "hdfs://namenode:9000/data/IT_2022_hourly.csv"
df = spark.read.csv(path_to_hdfs_file, header=True, inferSchema=True)

# 3. Parsing timestamp per convertire "Datetime (UTC)" in formato timestamp
df = df.withColumn("Datetime (UTC)", to_timestamp(col("Datetime (UTC)"), "yyyy-MM-dd HH:mm:ss"))

# 4. Estrazione anno
df = df.withColumn("Year", year("Datetime (UTC)"))

# 5. Calcolo media, minimo e massimo per carbon intensity e CFE%
agg_df = df.groupBy("Year").agg(
    avg(col("Carbon intensity gCO₂eq/kWh (direct)")).alias("carbon-mean"),
    min(col("Carbon intensity gCO₂eq/kWh (direct)")).alias("carbon-min"),
    max(col("Carbon intensity gCO₂eq/kWh (direct)")).alias("carbon-max"),

    avg(col("Carbon-free energy percentage (CFE%)")).alias("cfe-mean"),
    min(col("Carbon-free energy percentage (CFE%)")).alias("cfe-min"),
    max(col("Carbon-free energy percentage (CFE%)")).alias("cfe-max")
)

# 6. Aggiunta colonna con "Zone id"
agg_df = agg_df.withColumn("Zone id", lit("IT"))

# 7. Output dei risultati
agg_df.select("Zone id", "Year",
              "carbon-mean", "carbon-min", "carbon-max",
              "cfe-mean", "cfe-min", "cfe-max").show()

# 8. Salvataggio dei risultati su HDFS
agg_df.write.mode("overwrite").csv("hdfs://namenode:9000/results/query1") # Questo salva i dati su più partizioni

# Questo salva i dati su una sola partizione, più comodo nel nostro caso visto che i dati sono pochi e piccoli
# agg_df.coalesce(1).write.mode("overwrite").csv("hdfs://namenode:9000/results/query1") 



# TODO:
# 1 - vedere come evitare l'overwrite se si usano file diversi
# 2 - ottenere tutti gli output relativi a IT o a SE in un unico file (scegliamo poi se inserirli in IT.csv o SE.csv, oppure in un solo file)
# 3 - rendere tutto più dinamico e meno hard-coded
# 4 - attualmente Spark scrive su HDFS e lo fa usando una notazione come part-BLABLA.csv, dobbiamo vedere se ci conviene cambiarlo
# 5 - aggiungere i grafici
