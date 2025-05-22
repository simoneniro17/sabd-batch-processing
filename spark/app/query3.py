from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, col, date_format, avg, min, max, round, lit, percentile

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
    
    # Parsing del timestamp e estrazione di data per il raggruppamento temporale di 24 ore
    df = df.withColumn("Datetime (UTC)", to_timestamp(col("Datetime__UTC_"), "yyyy-MM-dd HH:mm:ss"))
    df = df.withColumn("date", date_format("Datetime (UTC)", "yyyy-MM-dd"))     # Data senza ora per raggruppare in base a 24 ore
    
    # Calcoliamo le aggregazioni per ciascun giorno per le due metriche e i valori richiesti
    # Castiamo le colonne numeriche per evitare errori di tipo (magari causati dalla conversione in Parquet durante il preprocessing)
    daily_avg = df.groupBy("date").agg(
        round(avg(col("Carbon_intensity_gCO_eq_kWh__direct_").cast("double")), 6).alias("carbon_intensity"),
        round(avg(col("Carbon_free_energy_percentage__CFE__").cast("double")), 6).alias("cfe_percentage")
    )
    
    # max_carbon_day = daily_avg.orderBy(col("carbon_intensity").desc()).first()
    # print(f"Il giorno con il valore più alto di carbon intensity in {zone_id} è {max_carbon_day['date']} con un valore di {max_carbon_day['carbon_intensity']} gCO2eq/kWh.")
    # min_carbon_day = daily_avg.orderBy(col("carbon_intensity").asc()).first()
    # print(f"Il giorno con il valore più basso di carbon intensity in {zone_id} è {min_carbon_day['date']} con un valore di {min_carbon_day['carbon_intensity']} gCO2eq/kWh.")
    # max_cfe_day = daily_avg.orderBy(col("cfe_percentage").desc()).first()
    # print(f"Il giorno con il valore più alto di CFE in {zone_id} è {max_cfe_day['date']} con un valore di {max_cfe_day['cfe_percentage']}%.")
    # min_cfe_day = daily_avg.orderBy(col("cfe_percentage").asc()).first()
    # print(f"Il giorno con il valore più basso di CFE in {zone_id} è {min_cfe_day['date']} con un valore di {min_cfe_day['cfe_percentage']}%.")

    # Calcolo delle statistiche min, max, e percentili per le due metriche
    stats = daily_avg.agg(
        min("carbon_intensity").alias("carbon_min"),
        percentile("carbon_intensity", 0.25).alias("carbon_25p"),
        percentile("carbon_intensity", 0.5).alias("carbon_50p"),
        percentile("carbon_intensity", 0.75).alias("carbon_75p"),
        max("carbon_intensity").alias("carbon_max"),
        
        min("cfe_percentage").alias("cfe_min"),
        percentile("cfe_percentage", 0.25).alias("cfe_25p"),
        percentile("cfe_percentage", 0.5).alias("cfe_50p"),
        percentile("cfe_percentage", 0.75).alias("cfe_75p"),
        max("cfe_percentage").alias("cfe_max")
    )

    # Aggiunta dell'identificatore della zona e separazione delle statistiche per le due metriche
    carbon_stats = stats.select(
        lit(zone_id).alias("country"),
        lit("carbon_intensity").alias("metric"),
        col("carbon_min").alias("min"),
        col("carbon_25p").alias("25-perc"),
        col("carbon_50p").alias("50-perc"),
        col("carbon_75p").alias("75-perc"),
        col("carbon_max").alias("max")
    )
    
    cfe_stats = stats.select(
        lit(zone_id).alias("country"),
        lit("cfe_percentage").alias("metric"),
        col("cfe_min").alias("min"),
        col("cfe_25p").alias("25-perc"),
        col("cfe_50p").alias("50-perc"),
        col("cfe_75p").alias("75-perc"),
        col("cfe_max").alias("max")
    )

    # Uniamo le statistiche per le due metriche del singolo paese in un unico DataFrame
    return carbon_stats.union(cfe_stats)


def main_query3(spark, input_it, input_se, output_path):

    # Processiamo i file per l'Italia e la Svezia
    it_results = process_file(spark, input_it, "IT")
    se_results = process_file(spark, input_se, "SE")

    # Uniamo i risultati dei due paesi in un unico DataFrame
    combined_df = it_results.unionByName(se_results)

    # Salvataggio del DataFrame finale
    combined_df.coalesce(1).write.mode("overwrite").option("header", True).csv(output_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Query 3: statistiche su medie giornaliere per Italia e Svezia.")
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

    # Inizializziamo la sessione Spark
    spark = SparkSession.builder.appName(f"Q3-IT-SE").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    try:
        # Istanziazione classe per la valutazione delle prestazioni
        evaluator = Evaluation(spark, args.runs, output, "query3", "DF")

        # Esecuzione e valutazione
        evaluator.run(main_query3, spark, input_it, input_se, output)
        evaluator.evaluate()
    except Exception as e:
        print(f"Errore durante l'elaborazione di Query 3: {e}")
        raise # Rilanciamo l'eccezione affinché le statistiche vengano calcolate solo se la query ha successo
    finally:
        spark.stop()