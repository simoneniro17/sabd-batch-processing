from pyspark.sql import SparkSession
from pyspark.sql.functions import year, avg, min, max, to_timestamp, col, lit, round

import argparse
from evaluation import Evaluation
import os


def process_file(spark, path, zone_id):
    # Carichiamo i dati da file Parquet
    df = spark.read.parquet(path)

    # Prima di procedere, verifichiamo che le colonne richieste siano presenti
    required_cols = ["Datetime__UTC_", "Carbon_intensity_gCO_eq_kWh__direct_", "Carbon_free_energy_percentage__CFE__"]
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        raise ValueError(f"Colonne mancanti nel dataset: {missing}")

    # Parsing del timestamp e estrazione dell'anno per il raggruppamento temporale
    df = df.withColumn("Datetime (UTC)", to_timestamp(col("Datetime__UTC_"), "yyyy-MM-dd HH:mm:ss"))
    df = df.withColumn("year", year("Datetime (UTC)"))

    # Calcoliamo le aggregazioni annuali per le due metriche e i valori richiesti
    # Castiamo le colonne numeriche per evitare errori di tipo (magari causati dalla conversione in Parquet durante il preprocessing)
    agg_df = df.groupBy("year").agg(
        round(avg(col("Carbon_intensity_gCO_eq_kWh__direct_").cast("double")), 6).alias("carbon-avg"),
        min(col("Carbon_intensity_gCO_eq_kWh__direct_").cast("double")).alias("carbon-min"),
        max(col("Carbon_intensity_gCO_eq_kWh__direct_").cast("double")).alias("carbon-max"),

        round(avg(col("Carbon_free_energy_percentage__CFE__").cast("double")), 6).alias("cfe-avg"),
        min(col("Carbon_free_energy_percentage__CFE__").cast("double")).alias("cfe-min"),
        max(col("Carbon_free_energy_percentage__CFE__").cast("double")).alias("cfe-max")
    )

    # Aggiungiamo la colonna per il paese
    return agg_df.withColumn("country", lit(zone_id))


def main(input_it, input_se, output_path):
    # Inizializziamo la sessione Spark
    spark = SparkSession.builder.appName(f"Q1-IT-SE").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    try:    
        # Processiamo separatamente i file per Italia e Svezia
        it_results = process_file(spark, input_it, "IT")
        se_results = process_file(spark, input_se, "SE")

        # Uniamo i risultati in un unico DataFrame
        combined_df = it_results.unionByName(se_results)

        # Selezioniamo e ordiniamo le colonne finali per maggiore leggibilità
        final_df = combined_df.select(
            "year",
            "country",
            "carbon-avg",
            "carbon-min",
            "carbon-max",
            "cfe-avg",
            "cfe-min",
            "cfe-max"
        ).orderBy("country", "year")

        final_df.show()

        # Salviamo il risultato finale in formato CSV su HDFS, sovrascrivendo eventuali file esistenti
        final_df.write.mode("overwrite").option("header", True).csv(output_path)
    except Exception as e:
        print(f"Errore durante l'elaborazione di Query1: {e}")
        raise # Rilanciamo l'eccezione affinché le statistiche vengano calcolate solo se la query ha successo
    finally:
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Query 1: Elaborazione dei dati di elettricità per zona e anno.")
    parser.add_argument("--input_it", required=True, help="Percorso per file IT Parquet su HDFS")
    parser.add_argument("--input_se", required=True, help="Percorsi per file SE Parquet su HDFS")
    parser.add_argument("--output", required=True, help="Cartella di output su HDFS")
    parser.add_argument("--runs", type=int, default=1, help="Numero di esecuzioni per la misurazione delle prestazioni")

    args = parser.parse_args()

    # Costruzione dei percorsi HDFS
    HDFS_BASE = os.getenv("HDFS_BASE")
    input_it = f"{HDFS_BASE.rstrip('/')}/{args.input_it.lstrip('/')}"
    input_se = f"{HDFS_BASE.rstrip('/')}/{args.input_se.lstrip('/')}"
    output = f"{HDFS_BASE.rstrip('/')}/{args.output.lstrip('/')}"

    # Avvio della misurazione e della valutazione delle performance
    evaluator = Evaluation(args.runs, query_type="DataFrame")

    # Esecuzione e valutazione
    evaluator.run(main, input_it, input_se, output)
    evaluator.evaluate()