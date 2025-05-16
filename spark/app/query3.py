from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, col, hour, date_format, avg, min, max, expr, lit, percentile_approx

from pyspark.sql.window import Window
import argparse
import time
import statistics

# ## Query 3 (Italia e Svezia)
# *   Aggregare i dati di ciascun paese su un **periodo di 24 ore**.
# *   Calcolare il valore medio (average) dell'**intensità di carbonio** e della **percentuale di energia a zero emissioni (CFE%)** 
#       per ogni periodo di 24 ore.
# *   Per questi valori medi calcolati sulle 24 ore, computare il minimo (minimum), il 25° percentile, il 50° percentile (mediana),
#       il 75° percentile e il massimo (maximum) per ciascuna delle due metriche (intensità di carbonio e CFE%).
# *   Considerando sempre il valor medio delle due metriche aggregati sulle 24 fasce orarie giornaliere, generare **due grafici** per
#       confrontare visivamente l'andamento (trend).
# ### Esempio di output
# ```
# # county, data, min, 25-perc, 50-perc, 75-perc, max
# IT, carbon-intensity, 219.029329, 241.060318, 279.202916, 285.008504, 296.746208
# IT, cfe, 42.203176, 45.728436, 47.600110, 53.149180, 57.423648
# SE, carbon-intensity, 3.150062, 3.765761, 4.293638, 4.876138, 5.947180
# SE, cfe, 99.213936, 99.338007, 99.411328, 99.472495, 99.540979
# ```

def process_file(spark, path, zone_id):
    df = spark.read.parquet(path)
    
    df = df.withColumn("Datetime (UTC)", to_timestamp(col("Datetime__UTC_"), "yyyy-MM-dd HH:mm:ss"))
    df = df.withColumn("Date", date_format("Datetime (UTC)", "yyyy-MM-dd"))     # Data senza ora per raggruppare in base a 24 ore
    
    daily_avg = df.groupBy("Date").agg(
        avg(col("Carbon_intensity_gCO_eq_kWh__direct_").cast("float")).alias("carbon_intensity"),
        avg(col("Carbon_free_energy_percentage__CFE__").cast("float")).alias("cfe_percentage")
    )
    
    # Calcolo di min, percentili e max
    stats = daily_avg.agg(
        # Carbon intensity
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

    return carbon_stats.union(cfe_stats)

def main(input_it, input_se, output_path):
    spark = SparkSession.builder.appName(f"Q3-IT-SE").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    execution_time = 0
    try:
        start_time = time.time()
        
        it_results = process_file(spark, input_it, "IT")
        se_results = process_file(spark, input_se, "SE")
            
        # Uniamo i risultati
        combined_df = it_results.unionByName(se_results)
        combined_df.show()
        
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
    parser.add_argument("--input_it", required=True, help="Percorso del file Parquet per Italia su HDFS")
    parser.add_argument("--input_se", required=True, help="Percorso del file Parquet per Svezia su HDFS")
    parser.add_argument("--output", required=True, help="Cartella di output su HDFS")
    parser.add_argument("--runs", type=int, default=1, help="Numero di esecuzioni per la misurazione delle prestazioni")

    args = parser.parse_args()

    if args.runs < 1:
        print("Il numero di esecuzioni (--runs) deve essere almeno 1.")
        exit(1)

    execution_times = []
    print(f"Avvio misurazione prestazioni per Query3 con {args.runs} esecuzioni...")

    for i in range(args.runs):
        run_time = main(args.input_it, args.input_se, args.output)
        if run_time > 0:
            execution_times.append(run_time)
        else:
            print(f"Errore durante l'esecuzione {i + 1}: tempo di esecuzione non valido.")

    if execution_times:
        mean_time = statistics.mean(execution_times)
        print(f"\nStatistiche prestazioni per Query3 dopo {len(execution_times)} esecuzioni valide:")
        print(f"Tempo medio di esecuzione: {mean_time:.4f} secondi")

        if len(execution_times) > 1:
            std_dev_time = statistics.stdev(execution_times)
            print(f"Deviazione standard: {std_dev_time:.4f} secondi")
        else:
            print("Deviazione standard non calcolabile con una sola esecuzione valida.")
    
        print(f"Tempi individuali registrati: {[round(t, 4) for t in execution_times]}")
    else:
        print("Nessuna esecuzione completata con successo, statistiche non disponibili.")