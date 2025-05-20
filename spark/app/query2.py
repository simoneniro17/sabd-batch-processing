from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, avg, col, to_timestamp, round, lit

import argparse
from evaluation import Evaluation
import os


def process_file(spark, path):
    # Carichiamo i dati da file Parquet
    df = spark.read.parquet(path)

    # Prima di procedere, verifichiamo che le colonne richieste siano presenti
    required_cols = ["Datetime__UTC_", "Carbon_intensity_gCO_eq_kWh__direct_", "Carbon_free_energy_percentage__CFE__"]
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        raise ValueError(f"Colonne mancanti nel dataset: {missing}")

    # Parsing del timestamp e estrazione di anno e mese per il raggruppamento temporale
    df = df.withColumn("Datetime (UTC)", to_timestamp(col("Datetime__UTC_"), "yyyy-MM-dd HH:mm:ss"))
    df = df.withColumn("year", year("Datetime (UTC)"))
    df = df.withColumn("month", month("Datetime (UTC)"))

    # Calcoliamo le aggregazioni per le coppie (anno, mese) per le due metriche e i valori richiesti
    # Castiamo le colonne numeriche per evitare errori di tipo (magari causati dalla conversione in Parquet durante il preprocessing)
    agg_df = df.groupBy("year", "month").agg(
        round(avg(col("Carbon_intensity_gCO_eq_kWh__direct_").cast("double")), 6).alias("carbon-avg"),
        round(avg(col("Carbon_free_energy_percentage__CFE__").cast("double")), 6).alias("cfe-avg"),
    )

    return agg_df


def calculate_rankings(df):
    # Ordiniamo il DataFrame per ciascuna metrica e direzione (asc/desc) per ottenere le prime 5 coppie
    highest_carbon = df.orderBy(col("carbon-avg").desc()).limit(5).withColumn("label", lit("highest_carbon"))
    lowest_carbon = df.orderBy(col("carbon-avg").asc()).limit(5).withColumn("label", lit("lowest_carbon"))
    highest_cfe = df.orderBy(col("cfe-avg").desc()).limit(5).withColumn("label", lit("highest_cfe"))
    lowest_cfe = df.orderBy(col("cfe-avg").asc()).limit(5).withColumn("label", lit("lowest_cfe"))

    return highest_carbon, lowest_carbon, highest_cfe, lowest_cfe


def main(input_path, output_path):
    # Inizializziamo la sessione Spark
    spark = SparkSession.builder.appName("Query2-IT").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    try:
        # Processiamo il file per l'Italia
        df = process_file(spark, input_path)

        # Calcoliamo le classifiche richieste
        highest_carbon, lowest_carbon, highest_cfe, lowest_cfe = calculate_rankings(df)

        # Uniamo i risultati delle quattro classifiche in un unico DataFrame
        final_df = highest_carbon \
            .unionByName(lowest_carbon) \
            .unionByName(highest_cfe) \
            .unionByName(lowest_cfe)
        
        final_df.show(truncate=False)

        # Se vogliamo salvare i risultati delle classifiche in file separati
        # highest_carbon.write.mode("overwrite").option("header", True).csv(f"{output_path}/highest_carbon")
        # lowest_carbon.write.mode("overwrite").option("header", True).csv(f"{output_path}/lowest_carbon")
        # highest_cfe.write.mode("overwrite").option("header", True).csv(f"{output_path}/highest_cfe")
        # lowest_cfe.write.mode("overwrite").option("header", True).csv(f"{output_path}/lowest_cfe")

        # Salvataggio del DataFrame finale
        final_df.coalesce(1).write.mode("overwrite").option("header", True).csv(output_path)
    except Exception as e:
        print(f"Errore durante l'elaborazione di Query2: {e}")
    finally:
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Query 2: Elaborazione dei dati di elettricità per anno e mese per l'Italia.")
    parser.add_argument("--input", required=True, help="Percorso per file IT Parquet su HDFS")
    parser.add_argument("--output", required=True, help="Cartella di output su HDFS")
    parser.add_argument("--runs", type=int, default=1, help="Numero di esecuzioni per la misurazione delle prestazioni")

    args = parser.parse_args()

    # Costruzione dei percorsi HDFS
    HDFS_BASE = os.getenv("HDFS_BASE")
    input = f"{HDFS_BASE.rstrip('/')}/{args.input.lstrip('/')}"
    output = f"{HDFS_BASE.rstrip('/')}/{args.output.lstrip('/')}"

    # Avvio della misurazione e della valutazione delle performance
    evaluator = Evaluation(args.runs)
    evaluator.run(main, input, output)
    evaluator.evaluate()