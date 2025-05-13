from pyspark.sql import SparkSession
from pyspark.sql.functions import year, avg, min, max, to_timestamp, col, lit
import argparse

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

def process_file(spark, path, zone_id):
    df = spark.read.csv(path, header=True, inferSchema=True)

    df = df.withColumn("Datetime (UTC)", to_timestamp(col("Datetime (UTC)"), "yyyy-MM-dd HH:mm:ss"))
    df = df.withColumn("Year", year("Datetime (UTC)"))

    agg_df = df.groupBy("Year").agg(
        avg(col("Carbon intensity gCO₂eq/kWh (direct)")).alias("carbon-mean"),
        min(col("Carbon intensity gCO₂eq/kWh (direct)")).alias("carbon-min"),
        max(col("Carbon intensity gCO₂eq/kWh (direct)")).alias("carbon-max"),

        avg(col("Carbon-free energy percentage (CFE%)")).alias("cfe-mean"),
        min(col("Carbon-free energy percentage (CFE%)")).alias("cfe-min"),
        max(col("Carbon-free energy percentage (CFE%)")).alias("cfe-max")
    )

    # Aggiungi zona (IT o SE)
    return agg_df.withColumn("Zone id", lit(zone_id))


def main(input_paths, output_path, zone_id):
    spark = SparkSession.builder.appName("ElectricityMaps-Batch-Processing").getOrCreate()

    # Split manuale dei percorsi
    file_list = input_paths.split(",")

    # Processa ogni file e unisci i risultati
    results = [process_file(spark, path.strip(), zone_id) for path in file_list]
    combined_df = results[0]
    for df in results[1:]:
        combined_df = combined_df.unionByName(df)

    # Aggrega ancora (per sicurezza), nel caso più file dello stesso anno
    final_df = combined_df.groupBy("Zone id", "Year").agg(
        avg("carbon-mean").alias("carbon-mean"),
        min("carbon-min").alias("carbon-min"),
        max("carbon-max").alias("carbon-max"),

        avg("cfe-mean").alias("cfe-mean"),
        min("cfe-min").alias("cfe-min"),
        max("cfe-max").alias("cfe-max")
    )

    final_df.show()

    # Scrivi in una sola partizione per ottenere un solo file
    final_df.coalesce(1).write.mode("overwrite").option("header", True).csv(output_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Aggrega i dati elettrici su base annua.")
    parser.add_argument("--input", required=True, help="Percorsi CSV separati da virgola su HDFS")
    parser.add_argument("--output", required=True, help="Cartella di output su HDFS")
    parser.add_argument("--zone", required=True, help="ID della zona (es: IT, SE)")

    args = parser.parse_args()
    main(args.input, args.output, args.zone)


# TODO:
# 2 - ottenere tutti gli output relativi a IT o a SE in un unico file (scegliamo poi se inserirli in IT.csv o SE.csv, oppure in un solo file)
# 4 - attualmente Spark scrive su HDFS e lo fa usando una notazione come part-BLABLA.csv, dobbiamo vedere se ci conviene cambiarlo
# 5 - aggiungere i grafici
