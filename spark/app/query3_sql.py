from pyspark.sql import SparkSession
import argparse

from evaluation import Evaluation

def process_country_sql(spark, input_path, zone_id, view_name_suffix):
    raw_df = spark.read.parquet(input_path)
    raw_view_name = f"raw_data_{view_name_suffix}"
    raw_df.createOrReplaceTempView(raw_view_name)

    # Vista per le medie giornaliere (periodo di 24 ore)
    daily_avg_view_name = f"daily_averages_{view_name_suffix}"
    daily_avg_query = f"""
        CREATE OR REPLACE TEMP VIEW {daily_avg_view_name} AS
        SELECT
            DATE(TO_TIMESTAMP(`Datetime__UTC_`, 'yyyy-MM-dd HH:mm:ss')) AS AggDate,
            AVG(CAST(`Carbon_intensity_gCO_eq_kWh__direct_` AS FLOAT)) AS avg_carbon_intensity,
            AVG(CAST(`Carbon_free_energy_percentage__CFE__` AS FLOAT)) AS avg_cfe_percentage
        FROM
            {raw_view_name}
        GROUP BY
            AggDate
    """
    spark.sql(daily_avg_query)

    # Query per calcolare min, percentili (25, 50, 75) e max sulle medie giornaliere
    # L'output è formattato con una riga per metrica (carbon_intensity, cfe_percentage)
    stats_query = f"""
        SELECT
            '{zone_id}' AS country,
            'carbon_intensity' AS metric,
            MIN(avg_carbon_intensity) AS `min`,
            percentile_approx(avg_carbon_intensity, 0.25) AS `25-perc`,
            percentile_approx(avg_carbon_intensity, 0.50) AS `50-perc`,
            percentile_approx(avg_carbon_intensity, 0.75) AS `75-perc`,
            MAX(avg_carbon_intensity) AS `max`
        FROM {daily_avg_view_name}

        UNION ALL

        SELECT
            '{zone_id}' AS country,
            'cfe_percentage' AS metric,
            MIN(avg_cfe_percentage) AS `min`,
            percentile_approx(avg_cfe_percentage, 0.25) AS `25-perc`,
            percentile_approx(avg_cfe_percentage, 0.50) AS `50-perc`,
            percentile_approx(avg_cfe_percentage, 0.75) AS `75-perc`,
            MAX(avg_cfe_percentage) AS `max`
        FROM {daily_avg_view_name}
    """
    return spark.sql(stats_query)

def main_sql_query3(input_it, input_se, output_path):
    spark = SparkSession.builder.appName("SQL-Query3-IT-SE").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    try:
        it_results_df = process_country_sql(spark, input_it, "IT", "it_sql_q3")
        se_results_df = process_country_sql(spark, input_se, "SE", "se_sql_q3")

        combined_df = it_results_df.unionByName(se_results_df)
        combined_df.show(truncate=False)

        combined_df.coalesce(1).write.mode("overwrite").option("header", True).csv(output_path)

    except Exception as e:
        print(f"Errore durante l'elaborazione di Query3 SQL: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Query 3 (Spark SQL): Statistiche su medie giornaliere per IT e SE.")
    parser.add_argument("--input_it", required=True, help="Percorso del file Parquet IT di input su HDFS")
    parser.add_argument("--input_se", required=True, help="Percorso del file Parquet SE di input su HDFS")
    parser.add_argument("--output", required=True, help="Cartella di output su HDFS")
    parser.add_argument("--runs", type=int, default=1, help="Numero di esecuzioni per la misurazione delle prestazioni")

    args = parser.parse_args()

    evaluator = Evaluation(args.runs)
    evaluator.run(main_sql_query3, args.input_it, args.input_se, args.output)
    evaluator.evaluate()