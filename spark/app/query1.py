from pyspark.sql import SparkSession
from pyspark.sql.functions import year, avg, min, max, to_timestamp, col, lit

import argparse
import time
import statistics

def process_file(spark, path, zone_id):
    df = spark.read.parquet(path)

    df = df.withColumn("Datetime (UTC)", to_timestamp(col("Datetime__UTC_"), "yyyy-MM-dd HH:mm:ss"))
    df = df.withColumn("Year", year("Datetime (UTC)"))

    agg_df = df.groupBy("Year").agg(
        avg(col("Carbon_intensity_gCO_eq_kWh__direct_")).alias("carbon-avg"),
        min(col("Carbon_intensity_gCO_eq_kWh__direct_")).alias("carbon-min"),
        max(col("Carbon_intensity_gCO_eq_kWh__direct_")).alias("carbon-max"),

        avg(col("Carbon_free_energy_percentage__CFE__")).alias("cfe-avg"),
        min(col("Carbon_free_energy_percentage__CFE__")).alias("cfe-min"),
        max(col("Carbon_free_energy_percentage__CFE__")).alias("cfe-max")
    )

    # Aggiungi zona (IT o SE)
    return agg_df.withColumn("Zone_id", lit(zone_id))


def main(input_paths, output_path, zone_id):
    spark = SparkSession.builder.appName(f"Q1-{zone_id}").getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    execution_time = 0
    try:
        start_time = time.time()
        
        # Split manuale dei percorsi
        file_list = input_paths.split(",")

        # Processa ogni file e unisci i risultati
        results = [process_file(spark, path.strip(), zone_id) for path in file_list]
        combined_df = results[0]
        for df in results[1:]:
            combined_df = combined_df.unionByName(df)

        # Aggrega ancora (per sicurezza), nel caso più file dello stesso anno
        final_df = combined_df.groupBy("Zone_id", "Year").agg(
            avg("carbon-avg").alias("carbon-avg"),
            min("carbon-min").alias("carbon-min"),
            max("carbon-max").alias("carbon-max"),

            avg("cfe-avg").alias("cfe-avg"),
            min("cfe-min").alias("cfe-min"),
            max("cfe-max").alias("cfe-max")
        )

        # final_df.show()

        final_df.write.mode("overwrite").option("header", True).csv(output_path)

        end_time = time.time()
        execution_time = end_time - start_time
    except Exception as e:
        print(f"Errore durante l'elaborazione: {e}")
    finally:
        spark.stop()
    return execution_time


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Query 1: Elaborazione dei dati di elettricità per zona e anno.")
    parser.add_argument("--input", required=True, help="Percorsi Parquet separati da virgola su HDFS")
    parser.add_argument("--output", required=True, help="Cartella di output su HDFS")
    parser.add_argument("--zone", required=True, help="ID della zona (es: IT, SE)")
    parser.add_argument("--runs", type=int, default=1, help="Numero di esecuzioni per la misurazione delle prestazioni")

    args = parser.parse_args()

    if args.runs < 1:
        print("Il numero di esecuzioni (--runs) deve essere almeno 1.")
        exit(1)

    execution_times = []
    print(f"Avvio misurazione prestazioni per Query1 ({args.zone}) con {args.runs} esecuzioni...")

    for i in range(args.runs):
        run_time = main(args.input, args.output, args.zone)
        if run_time > 0:
            execution_times.append(run_time)
        else:
            print(f"Errore durante l'esecuzione {i + 1}: tempo di esecuzione non valido.")

    if execution_times:
        mean_time = statistics.mean(execution_times)
        print(f"\nStatistiche prestazioni per Query1 ({args.zone}) dopo {len(execution_times)} esecuzioni valide:")
        print(f"Tempo medio di esecuzione: {mean_time:.4f} secondi")

        if len(execution_times) > 1:
            std_dev_time = statistics.stdev(execution_times)
            print(f"Deviazione standard: {std_dev_time:.4f} secondi")
        else:
            print("Deviazione standard non calcolabile con una sola esecuzione valida.")
    
        print(f"Tempi individuali registrati: {[round(t, 4) for t in execution_times]}")
    else:
        print("Nessuna esecuzione completata con successo, statistiche non disponibili.")
