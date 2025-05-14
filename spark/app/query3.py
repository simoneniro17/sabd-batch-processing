from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    to_timestamp, col, hour, date_format, 
    avg, min, max, expr, lit, percentile_approx
)
from pyspark.sql.window import Window
import argparse
import time
import statistics

def process_file(spark, path, zone_id):
# Leggi il file Parquet invece del CSV
    df = spark.read.parquet(path)
    
    # Il resto della funzione rimane uguale poiché la struttura del DataFrame non cambia
    df = df.withColumn("Datetime (UTC)", 
                      to_timestamp(col("Datetime__UTC_"), "yyyy-MM-dd HH:mm:ss"))
    
    # Estrai la data (senza ora) per raggruppare per periodi di 24 ore
    df = df.withColumn("Date", date_format("Datetime (UTC)", "yyyy-MM-dd"))
    
    # Calcola medie giornaliere
    daily_avg = df.groupBy("Date").agg(
        avg("Carbon_intensity_gCO_eq_kWh__direct_").alias("carbon_intensity"),
        avg("Carbon_free_energy_percentage__CFE__").alias("cfe_percentage")
    )
    
    # Calcola le statistiche richieste (min, percentili, max)
    stats = daily_avg.agg(
        # Carbon intensity stats
        min("carbon_intensity").alias("carbon_min"),
        expr("percentile_approx(carbon_intensity, array(0.25))")[0].alias("carbon_25p"),
        expr("percentile_approx(carbon_intensity, array(0.5))")[0].alias("carbon_50p"),
        expr("percentile_approx(carbon_intensity, array(0.75))")[0].alias("carbon_75p"),
        max("carbon_intensity").alias("carbon_max"),
        
        # CFE percentage stats
        min("cfe_percentage").alias("cfe_min"),
        expr("percentile_approx(cfe_percentage, array(0.25))")[0].alias("cfe_25p"),
        expr("percentile_approx(cfe_percentage, array(0.5))")[0].alias("cfe_50p"),
        expr("percentile_approx(cfe_percentage, array(0.75))")[0].alias("cfe_75p"),
        max("cfe_percentage").alias("cfe_max")
    )
    
    # Aggiungi l'identificatore della zona e crea il formato di output desiderato
    carbon_stats = stats.select(
        lit(zone_id).alias("country"),
        lit("carbon-intensity").alias("metric"),
        col("carbon_min").alias("min"),
        col("carbon_25p").alias("25-perc"),
        col("carbon_50p").alias("50-perc"),
        col("carbon_75p").alias("75-perc"),
        col("carbon_max").alias("max")
    )
    
    cfe_stats = stats.select(
        lit(zone_id).alias("country"),
        lit("cfe").alias("metric"),
        col("cfe_min").alias("min"),
        col("cfe_25p").alias("25-perc"),
        col("cfe_50p").alias("50-perc"),
        col("cfe_75p").alias("75-perc"),
        col("cfe_max").alias("max")
    )
    
    # Unisci i risultati
    return carbon_stats.union(cfe_stats)

def main(input_paths, output_path, zone_id):
    spark = SparkSession.builder.appName(f"Q3-{zone_id}").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    execution_time = 0
    try:
        start_time = time.time()
        
        # Split dei percorsi di input
        file_list = input_paths.split(",")
        
        # Processa ogni file e unisci i risultati
        results = [process_file(spark, path.strip(), zone_id) for path in file_list]
        combined_df = results[0]
        for df in results[1:]:
            combined_df = combined_df.unionByName(df)
            
        # Scrivi il risultato in formato CSV
        combined_df.coalesce(1).write.mode("overwrite").option("header", True).csv(output_path)
        
        end_time = time.time()
        execution_time = end_time - start_time
    except Exception as e:
        print(f"Errore durante l'elaborazione: {e}")
    finally:
        spark.stop()
    return execution_time

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Analisi statistica su periodi di 24 ore.")
    parser.add_argument("--input", required=True, help="Percorsi CSV separati da virgola su HDFS")
    parser.add_argument("--output", required=True, help="Cartella di output su HDFS")
    parser.add_argument("--zone", required=True, help="ID della zona (es: IT, SE)")
    parser.add_argument("--runs", type=int, default=1, 
                       help="Numero di esecuzioni per la misurazione delle prestazioni")

    args = parser.parse_args()

    if args.runs < 1:
        print("Il numero di esecuzioni (--runs) deve essere almeno 1.")
        exit(1)

    execution_times = []
    print(f"Avvio misurazione prestazioni per Query3 ({args.zone}) con {args.runs} esecuzioni...")

    for i in range(args.runs):
        run_time = main(args.input, args.output, args.zone)
        if run_time > 0:
            execution_times.append(run_time)
        else:
            print(f"Errore durante l'esecuzione {i + 1}: tempo di esecuzione non valido.")

    if execution_times:
        mean_time = statistics.mean(execution_times)
        print(f"\nStatistiche prestazioni per Query3 ({args.zone}) dopo {len(execution_times)} esecuzioni valide:")
        print(f"Tempo medio di esecuzione: {mean_time:.4f} secondi")

        if len(execution_times) > 1:
            std_dev_time = statistics.stdev(execution_times)
            print(f"Deviazione standard: {std_dev_time:.4f} secondi")
        else:
            print("Deviazione standard non calcolabile con una sola esecuzione valida.")
    
        print(f"Tempi individuali registrati: {[round(t, 4) for t in execution_times]}")
    else:
        print("Nessuna esecuzione completata con successo, statistiche non disponibili.")