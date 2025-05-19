from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
import argparse
from evaluation import Evaluation 
import os

def process_file_sql(spark, path, zone_id, view_name_suffix):
    df = spark.read.parquet(path)
    view_name = f"electricity_data_{view_name_suffix}"
    df.createOrReplaceTempView(view_name)

    # Query SQL per calcolare i risultati e aggregare i dati annualmente
    query = f"""
        SELECT
            YEAR(TO_TIMESTAMP(`Datetime__UTC_`, 'yyyy-MM-dd HH:mm:ss')) AS Year,
            AVG(CAST(`Carbon_intensity_gCO_eq_kWh__direct_` AS FLOAT)) AS `carbon-avg`,
            MIN(CAST(`Carbon_intensity_gCO_eq_kWh__direct_` AS FLOAT)) AS `carbon-min`,
            MAX(CAST(`Carbon_intensity_gCO_eq_kWh__direct_` AS FLOAT)) AS `carbon-max`,
            AVG(CAST(`Carbon_free_energy_percentage__CFE__` AS FLOAT)) AS `cfe-avg`,
            MIN(CAST(`Carbon_free_energy_percentage__CFE__` AS FLOAT)) AS `cfe-min`,
            MAX(CAST(`Carbon_free_energy_percentage__CFE__` AS FLOAT)) AS `cfe-max`
        FROM
            {view_name}
        GROUP BY
            Year
    """
    aggregated_df = spark.sql(query)
    return aggregated_df.withColumn("Zone_id", lit(zone_id))

def main_sql_query1(input_it, input_se, output_path):
    spark = SparkSession.builder.appName("SQL-Query1-IT-SE").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    try:
        it_results_df = process_file_sql(spark, input_it, "IT", "it_sql_q1")
        se_results_df = process_file_sql(spark, input_se, "SE", "se_sql_q1")

        combined_df = it_results_df.unionByName(se_results_df)

        final_df = combined_df.select(
            "Year",
            "Zone_id",
            "`carbon-avg`",
            "`carbon-min`",
            "`carbon-max`",
            "`cfe-avg`",
            "`cfe-min`",
            "`cfe-max`"
        ).orderBy("Zone_id", "Year")

        final_df.show()
        final_df.coalesce(1).write.mode("overwrite").option("header", True).csv(output_path)
    except Exception as e:
        print(f"Errore durante l'elaborazione di Query1 SQL: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Query 1 (SparkSQL): Elaborazione dei dati di elettricità per zona e anno.")
    parser.add_argument("--input_it", required=True, help="Percorso del file Parquet IT di input su HDFS")
    parser.add_argument("--input_se", required=True, help="Percorso del file Parquet SE di input su HDFS")
    parser.add_argument("--output", required=True, help="Cartella di output su HDFS")
    parser.add_argument("--runs", type=int, default=1, help="Numero di esecuzioni per la misurazione delle prestazioni")

    args = parser.parse_args()

    HDFS_BASE = os.getenv("HDFS_BASE")
    input_it = f"{HDFS_BASE.rstrip('/')}/{args.input_it.lstrip('/')}"
    input_se = f"{HDFS_BASE.rstrip('/')}/{args.input_se.lstrip('/')}"
    output = f"{HDFS_BASE.rstrip('/')}/{args.output.lstrip('/')}"

    evaluator = Evaluation(args.runs)
    evaluator.run(main_sql_query1, input_it, input_se, output)
    evaluator.evaluate()