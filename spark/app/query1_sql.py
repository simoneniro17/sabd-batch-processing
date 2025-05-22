from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, round

import argparse
from evaluation import Evaluation 
import os

def process_file_sql(spark, path, zone_id, view_name_suffix):
    # Carichiamo i dati da file Parquet
    df = spark.read.parquet(path)

    # Prima di procedere, verifichiamo che le colonne richieste siano presenti
    required_cols = ["Datetime__UTC_", "Carbon_intensity_gCO_eq_kWh__direct_", "Carbon_free_energy_percentage__CFE__"]
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        raise ValueError(f"Colonne mancanti nel dataset: {missing}")

    # Registriamo una vista temporanea che verrà interrogata da SparkSQL
    view_name = f"electricity_data_{view_name_suffix}"  # Il suffisso nel nome dovrebbe garantire l'assenza di conflitti in eventuali esecuzioni parallele
    df.createOrReplaceTempView(view_name)

    # Query SQL per aggregare aggregare i dati annualmente e calcolare i risultati
    # Castiamo le colonne numeriche per evitare errori di tipo (magari causati dalla conversione in Parquet durante il preprocessing)
    query = f"""
        SELECT
            YEAR(TO_TIMESTAMP(`Datetime__UTC_`, 'yyyy-MM-dd HH:mm:ss')) AS year,
            ROUND(AVG(CAST(`Carbon_intensity_gCO_eq_kWh__direct_` AS DOUBLE)), 6) AS `carbon-avg`,
            MIN(CAST(`Carbon_intensity_gCO_eq_kWh__direct_` AS DOUBLE)) AS `carbon-min`,
            MAX(CAST(`Carbon_intensity_gCO_eq_kWh__direct_` AS DOUBLE)) AS `carbon-max`,
            ROUND(AVG(CAST(`Carbon_free_energy_percentage__CFE__` AS DOUBLE)), 6) AS `cfe-avg`,
            MIN(CAST(`Carbon_free_energy_percentage__CFE__` AS DOUBLE)) AS `cfe-min`,
            MAX(CAST(`Carbon_free_energy_percentage__CFE__` AS DOUBLE)) AS `cfe-max`
        FROM
            {view_name}
        GROUP BY
            year
    """
    aggregated_df = spark.sql(query)

    # Aggiungiamo la colonna per il paese
    return aggregated_df.withColumn("country", lit(zone_id))

def main_sql_query1(spark, input_it, input_se, output_path):
        # Processiamo separatamente i file per Italia e Svezia
        it_results_df = process_file_sql(spark, input_it, "IT", "it_sql_q1")
        se_results_df = process_file_sql(spark, input_se, "SE", "se_sql_q1")

        # Uniamo i risultati in un unico DataFrame
        combined_df = it_results_df.unionByName(se_results_df)

        # Selezioniamo e ordiniamo le colonne finali per maggiore leggibilità
        final_df = combined_df.select(
            "year",
            "country",
            "`carbon-avg`",
            "`carbon-min`",
            "`carbon-max`",
            "`cfe-avg`",
            "`cfe-min`",
            "`cfe-max`"
        ).orderBy("country", "year")

        final_df.show()

        # Scriviamo il risultato finale in formato CSV su HDFS, sovrascrivendo eventuali file esistenti
        final_df.write.mode("overwrite").option("header", True).csv(output_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Query 1 (SparkSQL): Elaborazione dei dati di elettricità per zona e anno.")
    parser.add_argument("--input_it", required=True, help="Percorso del file Parquet IT di input su HDFS")
    parser.add_argument("--input_se", required=True, help="Percorso del file Parquet SE di input su HDFS")
    parser.add_argument("--output", required=True, help="Cartella di output su HDFS")
    parser.add_argument("--runs", type=int, default=1, help="Numero di esecuzioni per la misurazione delle prestazioni")

    args = parser.parse_args()

    # Costruzione dei percorsi HDFS
    HDFS_BASE = os.getenv("HDFS_BASE")
    input_it = f"{HDFS_BASE.rstrip('/')}/{args.input_it.lstrip('/')}"
    input_se = f"{HDFS_BASE.rstrip('/')}/{args.input_se.lstrip('/')}"
    output = f"{HDFS_BASE.rstrip('/')}/{args.output.lstrip('/')}"

    # Inizializziamo la sessione Spark
    spark = SparkSession.builder.appName("SQL-Query1-IT-SE").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    try: 
        # Istanziazione classe per la valutazione delle prestazioni
        evaluator = Evaluation(spark, args.runs, output, "query1-sql", "SQL")

        # Esecuzione e valutazione
        evaluator.run(main_sql_query1, spark, input_it, input_se, output)
        evaluator.evaluate()
    except Exception as e:
        print(f"Errore durante l'elaborazione di Query1 SQL: {e}")
        raise # Rilanciamo l'eccezione affinché le statistiche vengano calcolate solo se la query ha successo
    finally:
        spark.stop()