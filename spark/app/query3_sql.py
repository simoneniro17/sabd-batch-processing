from pyspark.sql import SparkSession

import argparse
from evaluation import Evaluation
import os


def process_country_sql(spark, input_path, zone_id, view_name_suffix):
    # Carichiamo i dati da file Parquet
    raw_df = spark.read.parquet(input_path)

    # Prima di procedere, verifichiamo che le colonne richieste siano presenti
    required_cols = ["Datetime__UTC_", "Carbon_intensity_gCO_eq_kWh__direct_", "Carbon_free_energy_percentage__CFE__"]
    missing = [col for col in required_cols if col not in raw_df.columns]
    if missing:
        raise ValueError(f"Colonne mancanti nel dataset: {missing}")
    
    # Registriamo una prima vista temporanea che verrà interrogata da SparkSQL per aggregare i dati
    raw_view_name = f"raw_data_{view_name_suffix}"  # Il suffisso nel nome dovrebbe garantire l'assenza di conflitti in eventuali esecuzioni parallele
    raw_df.createOrReplaceTempView(raw_view_name)

    # Query SQL per registrare un'ulteriore vista temporanea con le medie per le 24 fasce orarie
    hourly_avg_view_name = f"hourly_averages_{view_name_suffix}"
    hourly_avg_query = f"""
        CREATE OR REPLACE TEMP VIEW {hourly_avg_view_name} AS
        SELECT
            HOUR(TO_TIMESTAMP(`Datetime__UTC_`, 'yyyy-MM-dd HH:mm:ss')) AS hour_of_day,
            ROUND(AVG(CAST(`Carbon_intensity_gCO_eq_kWh__direct_` AS DOUBLE)), 6) AS avg_carbon_intensity,
            ROUND(AVG(CAST(`Carbon_free_energy_percentage__CFE__` AS DOUBLE)), 6) AS avg_cfe_percentage
        FROM
            {raw_view_name}
        GROUP BY
            hour_of_day
    """
    spark.sql(hourly_avg_query)

    # Query per calcolare min, percentili (25, 50, 75) e max sulle medie delle 24 fasce orarie
    # L'output è formattato con una riga per metrica (carbon_intensity, cfe_percentage)
    stats_query = f"""
        SELECT
            '{zone_id}' AS country,
            'carbon_intensity' AS metric,
            MIN(avg_carbon_intensity) AS `min`,
            ROUND(percentile(avg_carbon_intensity, 0.25), 6) AS `25-perc`,
            ROUND(percentile(avg_carbon_intensity, 0.50), 6) AS `50-perc`,
            ROUND(percentile(avg_carbon_intensity, 0.75), 6) AS `75-perc`,
            MAX(avg_carbon_intensity) AS `max`
        FROM {hourly_avg_view_name}

        UNION ALL

        SELECT
            '{zone_id}' AS country,
            'cfe_percentage' AS metric,
            MIN(avg_cfe_percentage) AS `min`,
            ROUND(percentile(avg_cfe_percentage, 0.25), 6) AS `25-perc`,
            ROUND(percentile(avg_cfe_percentage, 0.50), 6) AS `50-perc`,
            ROUND(percentile(avg_cfe_percentage, 0.75), 6) AS `75-perc`,
            MAX(avg_cfe_percentage) AS `max`
        FROM {hourly_avg_view_name}
    """
    return spark.sql(stats_query)


def main_sql_query3(spark,input_it, input_se, output_path):

    # Processiamo i file per l'Italia e la Svezia
    it_results_df = process_country_sql(spark, input_it, "IT", "it_sql_q3")
    se_results_df = process_country_sql(spark, input_se, "SE", "se_sql_q3")

    # Uniamo i risultati per le due zone in un unico DataFrame
    combined_df = it_results_df.unionByName(se_results_df)

    # Scriviamo il risultato finale in un file CSV su HDFS
    combined_df.coalesce(1).write.mode("overwrite").option("header", True).csv(output_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Query 3 (Spark SQL): statistiche sulle medie dei dati aggregati sulle 24h della giornata per Italia e Svezia.")
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

    # Inizializzaziamo la sessione Spark
    spark = SparkSession.builder.appName("SQL-Query3-IT-SE").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    try:
        # Istanziazione classe per la valutazione delle prestazioni
        evaluator = Evaluation(spark, args.runs, output, "query3-sql", "SQL")

        # Esecuzione e valutazione
        evaluator.run(main_sql_query3,spark, input_it, input_se, output)
        evaluator.evaluate()
    except Exception as e:
        print(f"Errore durante l'elaborazione di Query3 SQL: {e}")
        raise # Rilanciamo l'eccezione affinché le statistiche vengano calcolate solo se la query ha successo
    finally:
        spark.stop()