from pyspark.sql import SparkSession

import argparse
from evaluation import Evaluation
import os


def main_sql_query2(input_path, output_path):
    # Inizializziamo la sessione Spark
    spark = SparkSession.builder.appName("SQL-Query2-IT").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    try:
        # Carichiamo i dati da file Parquet
        df = spark.read.parquet(input_path)

        # Prima di procedere, verifichiamo che le colonne richieste siano presenti
        required_cols = ["Datetime__UTC_", "Carbon_intensity_gCO_eq_kWh__direct_", "Carbon_free_energy_percentage__CFE__"]
        missing = [col for col in required_cols if col not in df.columns]
        if missing:
            raise ValueError(f"Colonne mancanti nel dataset: {missing}")
    
        # Creiamo una vista temporanea per eseguire query SQL
        df.createOrReplaceTempView("electricity_data_it")

        # Query SQL per aggregare i dati per coppie (anno, mese) e calcolare la media dell'intensità di carbonio e della CFE%
        aggregated_query = """
            SELECT
                YEAR(TO_TIMESTAMP(`Datetime__UTC_`, 'yyyy-MM-dd HH:mm:ss')) AS year,
                MONTH(TO_TIMESTAMP(`Datetime__UTC_`, 'yyyy-MM-dd HH:mm:ss')) AS month,
                ROUND(AVG(CAST(`Carbon_intensity_gCO_eq_kWh__direct_` AS DOUBLE)), 6) AS `carbon-avg`,
                ROUND(AVG(CAST(`Carbon_free_energy_percentage__CFE__` AS DOUBLE)), 6) AS `cfe-avg`
            FROM
                electricity_data_it
            GROUP BY
                year, month
        """
        agg_df = spark.sql(aggregated_query)
        
        # Vista temporanea dai dati aggregati per facilitare le query di ranking
        agg_df.createOrReplaceTempView("monthly_averages_it")

        highest_carbon_query = """
            SELECT year, month, `carbon-avg`, `cfe-avg`, 'highest_carbon' AS label
            FROM monthly_averages_it
            ORDER BY `carbon-avg` DESC
            LIMIT 5
        """
        highest_carbon_df = spark.sql(highest_carbon_query)

        lowest_carbon_query = """
            SELECT year, month, `carbon-avg`, `cfe-avg`, 'lowest_carbon' AS label
            FROM monthly_averages_it
            ORDER BY `carbon-avg` ASC
            LIMIT 5
        """
        lowest_carbon_df = spark.sql(lowest_carbon_query)

        highest_cfe_query = """
            SELECT year, month, `carbon-avg`, `cfe-avg`, 'highest_cfe' AS label
            FROM monthly_averages_it
            ORDER BY `cfe-avg` DESC
            LIMIT 5
        """
        highest_cfe_df = spark.sql(highest_cfe_query)

        lowest_cfe_query = """
            SELECT year, month, `carbon-avg`, `cfe-avg`, 'lowest_cfe' AS label
            FROM monthly_averages_it
            ORDER BY `cfe-avg` ASC
            LIMIT 5
        """
        lowest_cfe_df = spark.sql(lowest_cfe_query)

        # Uniamo i risultati delle quattro classifiche in un unico DataFrame
        final_df = highest_carbon_df \
            .unionByName(lowest_carbon_df) \
            .unionByName(highest_cfe_df) \
            .unionByName(lowest_cfe_df)
        
        final_df.coalesce(1).write.mode("overwrite").option("header", True).csv(output_path)
    except Exception as e:
        print(f"Errore durante l'elaborazione di Query2 SQL: {e}")
        raise # Rilanciamo l'eccezione affinché le statistiche vengano calcolate solo se la query ha successo
    finally:
        spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Query 2 (SparkSQL): Elaborazione dei dati di elettricità per anno e mese (solo IT).")
    parser.add_argument("--input", required=True, help="Percorso del file Parquet di input su HDFS (dati IT)")
    parser.add_argument("--output", required=True, help="Cartella di output su HDFS")
    parser.add_argument("--runs", type=int, default=1, help="Numero di esecuzioni per la misurazione delle prestazioni")

    args = parser.parse_args()

    # Costruzione dei percorsi HDFS
    HDFS_BASE = os.getenv("HDFS_BASE")
    input = f"{HDFS_BASE.rstrip('/')}/{args.input.lstrip('/')}"
    output = f"{HDFS_BASE.rstrip('/')}/{args.output.lstrip('/')}"

    # Istanziazione classe per la valutazione delle prestazioni
    evaluator = Evaluation(args.runs, query_type="SQL")

    # Esecuzione e valutazione
    evaluator.run(main_sql_query2, input, output)
    evaluator.evaluate()