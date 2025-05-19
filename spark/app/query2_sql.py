from pyspark.sql import SparkSession
import argparse
import os
from evaluation import Evaluation

def main_sql_query2(input_path, output_path):
    spark = SparkSession.builder.appName("SQL-Query2-IT").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    try:
        df = spark.read.parquet(input_path)
        df.createOrReplaceTempView("electricity_data_it")

        # Query SQL per aggregare i dati per anno e mese
        # Calcola la media dell'intensità di carbonio e della CFE%
        aggregated_query = """
            SELECT
                YEAR(TO_TIMESTAMP(`Datetime__UTC_`, 'yyyy-MM-dd HH:mm:ss')) AS Year,
                MONTH(TO_TIMESTAMP(`Datetime__UTC_`, 'yyyy-MM-dd HH:mm:ss')) AS Month,
                AVG(CAST(`Carbon_intensity_gCO_eq_kWh__direct_` AS FLOAT)) AS `carbon-avg`,
                AVG(CAST(`Carbon_free_energy_percentage__CFE__` AS FLOAT)) AS `cfe-avg`
            FROM
                electricity_data_it
            GROUP BY
                Year, Month
        """
        agg_df = spark.sql(aggregated_query)
        
        # Vista temporanea dai dati aggregati per facilitare le query di ranking
        agg_df.createOrReplaceTempView("monthly_averages_it")

        # Query per le prime 5 coppie (anno, mese) con intensità di carbonio più alta
        highest_carbon_query = """
            SELECT Year, Month, `carbon-avg`, `cfe-avg`
            FROM monthly_averages_it
            ORDER BY `carbon-avg` DESC
            LIMIT 5
        """
        highest_carbon_df = spark.sql(highest_carbon_query)

        # Query per le prime 5 coppie (anno, mese) con intensità di carbonio più bassa
        lowest_carbon_query = """
            SELECT Year, Month, `carbon-avg`, `cfe-avg`
            FROM monthly_averages_it
            ORDER BY `carbon-avg` ASC
            LIMIT 5
        """
        lowest_carbon_df = spark.sql(lowest_carbon_query)

        # Query per le prime 5 coppie (anno, mese) con CFE% più alta
        highest_cfe_query = """
            SELECT Year, Month, `carbon-avg`, `cfe-avg`
            FROM monthly_averages_it
            ORDER BY `cfe-avg` DESC
            LIMIT 5
        """
        highest_cfe_df = spark.sql(highest_cfe_query)

        # Query per le prime 5 coppie (anno, mese) con CFE% più bassa
        lowest_cfe_query = """
            SELECT Year, Month, `carbon-avg`, `cfe-avg`
            FROM monthly_averages_it
            ORDER BY `cfe-avg` ASC
            LIMIT 5
        """
        lowest_cfe_df = spark.sql(lowest_cfe_query)

        print("Top 5 Highest Carbon Intensity:")
        highest_carbon_df.show()
        print("Top 5 Lowest Carbon Intensity:")
        lowest_carbon_df.show()
        print("Top 5 Highest CFE%:")
        highest_cfe_df.show()
        print("Top 5 Lowest CFE%:")
        lowest_cfe_df.show()

        # Unisce tutti i risultati per l'output finale (unionByName assicura che le colonne siano unite correttamente per nome)
        final_df = highest_carbon_df \
            .unionByName(lowest_carbon_df) \
            .unionByName(highest_cfe_df) \
            .unionByName(lowest_cfe_df)
        
        final_df.coalesce(1).write.mode("overwrite").option("header", True).csv(output_path)
    except Exception as e:
        print(f"Errore durante l'elaborazione di Query2 SQL: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Query 2 (SparkSQL): Elaborazione dei dati di elettricità per anno e mese (solo IT).")
    parser.add_argument("--input", required=True, help="Percorso del file Parquet di input su HDFS (dati IT)")
    parser.add_argument("--output", required=True, help="Cartella di output su HDFS")
    parser.add_argument("--runs", type=int, default=1, help="Numero di esecuzioni per la misurazione delle prestazioni")

    args = parser.parse_args()

    HDFS_BASE = os.getenv("HDFS_BASE")
    input = f"{HDFS_BASE.rstrip('/')}/{args.input.lstrip('/')}"
    output = f"{HDFS_BASE.rstrip('/')}/{args.output.lstrip('/')}"

    evaluator = Evaluation(args.runs)
    evaluator.run(main_sql_query2, input, output)
    evaluator.evaluate()