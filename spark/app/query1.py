from pyspark.sql import SparkSession
from pyspark.sql.functions import year, avg, min, max, to_timestamp, col, lit

import argparse
from evaluation import Evaluation

def process_file(spark, path, zone_id):
    df = spark.read.parquet(path)

    df = df.withColumn("Datetime (UTC)", to_timestamp(col("Datetime__UTC_"), "yyyy-MM-dd HH:mm:ss"))
    df = df.withColumn("Year", year("Datetime (UTC)"))

    agg_df = df.groupBy("Year").agg(
        avg(col("Carbon_intensity_gCO_eq_kWh__direct_").cast("float")).alias("carbon-avg"),
        min(col("Carbon_intensity_gCO_eq_kWh__direct_").cast("float")).alias("carbon-min"),
        max(col("Carbon_intensity_gCO_eq_kWh__direct_").cast("float")).alias("carbon-max"),

        avg(col("Carbon_free_energy_percentage__CFE__").cast("float")).alias("cfe-avg"),
        min(col("Carbon_free_energy_percentage__CFE__").cast("float")).alias("cfe-min"),
        max(col("Carbon_free_energy_percentage__CFE__").cast("float")).alias("cfe-max")
    )

    return agg_df.withColumn("Zone_id", lit(zone_id))


def main(input_it, input_se, output_path):
    spark = SparkSession.builder.appName(f"Q1-IT-SE").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    try:    
        # Processiamo sia i file IT che quelli SE
        it_results = process_file(spark, input_it, "IT")
        se_results = process_file(spark, input_se, "SE")

        # Uniamo i risultati
        combined_df = it_results.unionByName(se_results)

        final_df = combined_df.select(
            "Year",
            "Zone_id",
            "carbon-avg",
            "carbon-min",
            "carbon-max",
            "cfe-avg",
            "cfe-min",
            "cfe-max"
        ).orderBy("Zone_id", "Year")

        final_df.show()

        final_df.coalesce(1).write.mode("overwrite").option("header", True).csv(output_path)
    except Exception as e:
        print(f"Errore durante l'elaborazione: {e}")
    finally:
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Query 1: Elaborazione dei dati di elettricità per zona e anno.")
    parser.add_argument("--input_it", required=True, help="Percorso per file IT Parquet su HDFS")
    parser.add_argument("--input_se", required=True, help="Percorsi per file SE Parquet su HDFS")
    parser.add_argument("--output", required=True, help="Cartella di output su HDFS")
    parser.add_argument("--runs", type=int, default=1, help="Numero di esecuzioni per la misurazione delle prestazioni")

    args = parser.parse_args()

    evaluator = Evaluation(args.runs)
    evaluator.run(main, args.input_it, args.input_se, args.output)
    evaluator.evaluate()