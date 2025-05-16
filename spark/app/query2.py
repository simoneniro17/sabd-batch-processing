from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, avg, col, lit, to_timestamp

import argparse
import time
import statistics

## Query 2 (Solo Italia)
# *   Aggregare i dati sulla coppia (anno, mese) calcolando il valor medio dell'**intensità di carbonio** e della **percentuale di
#       energia a zero emissioni (CFE%)**.
# *   Calcolare la classifica (sempre usando Spark, no sorting manuale) delle **prime 5 coppie (anno, mese)** con:
#     *   Intensità di carbonio più alta (highest).
#     *   Intensità di carbonio più bassa (lowest).
#     *   Quota di energia a zero emissioni (CFE) più bassa (lowest).
#     *   Quota di energia a zero emissioni (CFE) più alta (highest).
#     In totale sono attesi 20 valori.
# *   Considerando il valor medio delle due metriche aggregati sulla coppia (anno, mese), generare **due grafici** per valutare
#       visivamente  l'andamento (trend) delle due metriche.
# ### Esempio di output
# ```
# # date, carbon-intensity, cfe
# 2022 12, 360.520000, 35.838320
# 2022 3, 347.359073, 35.822218
# 2021 11, 346.728514, 33.076681
# 2022 10, 335.784745, 39.167164
# 2022 2, 330.489896, 38.980595

# 2024 5, 158.240887, 68.989731
# 2024 4, 170.670889, 66.253958
# 2024 6, 171.978792, 65.487792
# 2024 3, 192.853871, 60.919556
# 2024 7, 200.595995, 57.939099

# 2024 5, 158.240887, 68.989731 
# 2024 4, 170.670889, 66.253958 
# 2024 6, 171.978792, 65.487792
# 2024 3, 192.853871, 60.919556
# 2023 5, 203.494489, 59.877003

# 2021 11, 346.728514, 33.076681
# 2022 3, 347.359073, 35.822218
# 2022 12, 360.520000, 35.838320
# 2022 1, 326.947876, 36.603683
# 2021 12, 329.303508, 37.868817
# ```

def process_file(spark, path):
    df = spark.read.parquet(path)

    # Colonne per anno e mese
    df = df.withColumn("Datetime (UTC)", to_timestamp(col("Datetime__UTC_"), "yyyy-MM-dd HH:mm:ss"))
    df = df.withColumn("Year", year("Datetime (UTC)"))
    df = df.withColumn("Month", month("Datetime (UTC)"))

    # Aggregazione dati per le coppie (anno, mese)
    agg_df = df.groupBy("Year", "Month").agg(
        avg(col("Carbon_intensity_gCO_eq_kWh__direct_").cast("float")).alias("carbon-avg"),
        avg(col("Carbon_free_energy_percentage__CFE__").cast("float")).alias("cfe-avg"),
    )

    return agg_df


def calculate_rankings(df):
    # Prime 5 coppie con intensità di carbonio più alta
    highest_carbon = df.orderBy(col("carbon-avg").desc()).limit(5)

    # Prime 5 coppie con intensità di carbonio più bassa
    lowest_carbon = df.orderBy(col("carbon-avg").asc()).limit(5)

    # Prime 5 coppie con CFE% più alta
    highest_cfe = df.orderBy(col("cfe-avg").desc()).limit(5)

    # Prime 5 coppie con CFE% più bassa
    lowest_cfe = df.orderBy(col("cfe-avg").asc()).limit(5)

    return highest_carbon, lowest_carbon, highest_cfe, lowest_cfe


def main(input_path, output_path):
    spark = SparkSession.builder.appName("Query2-IT").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    execution_time = 0
    try:
        start_time = time.time()

        # Processamento del file di input
        df = process_file(spark, input_path)

        # Calcolo delle classifiche
        highest_carbon, lowest_carbon, highest_cfe, lowest_cfe = calculate_rankings(df)

        highest_carbon.show()
        lowest_carbon.show()
        highest_cfe.show()
        lowest_cfe.show()

        final_df = highest_carbon \
            .unionByName(lowest_carbon) \
            .unionByName(highest_cfe) \
            .unionByName(lowest_cfe)
        
        # Se vogliamo salvare i risultati delle classifiche in file separati
        # highest_carbon.write.mode("overwrite").option("header", True).csv(f"{output_path}/highest_carbon")
        # lowest_carbon.write.mode("overwrite").option("header", True).csv(f"{output_path}/lowest_carbon")
        # highest_cfe.write.mode("overwrite").option("header", True).csv(f"{output_path}/highest_cfe")
        # lowest_cfe.write.mode("overwrite").option("header", True).csv(f"{output_path}/lowest_cfe")

        # Salvataggio del DataFrame finale
        final_df.coalesce(1).write.mode("overwrite").option("header", True).csv(output_path)

        end_time = time.time()
        execution_time = end_time - start_time
    except Exception as e:
        print(f"Errore durante l'elaborazione: {e}")
    finally:
        spark.stop()
    return execution_time


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Query 2: Elaborazione dei dati di elettricità per anno e mese.")
    parser.add_argument("--input", required=True, help="Percorso del file Parquet di input su HDFS")
    parser.add_argument("--output", required=True, help="Cartella di output su HDFS")
    parser.add_argument("--runs", type=int, default=1, help="Numero di esecuzioni per la misurazione delle prestazioni")

    args = parser.parse_args()

    if args.runs < 1:
        print("Il numero di esecuzioni (--runs) deve essere almeno 1.")
        exit(1)

    execution_times = []
    print(f"Avvio misurazione prestazioni per Query2 con {args.runs} esecuzioni...")

    for i in range(args.runs):
        run_time = main(args.input, args.output)
        if run_time > 0:
            execution_times.append(run_time)
        else:
            print(f"Errore durante l'esecuzione {i + 1}: tempo di esecuzione non valido.")

    if execution_times:
        mean_time = statistics.mean(execution_times)
        print(f"\nStatistiche prestazioni per Query2 dopo {len(execution_times)} esecuzioni valide:")
        print(f"Tempo medio di esecuzione: {mean_time:.4f} secondi")

        if len(execution_times) > 1:
            std_dev_time = statistics.stdev(execution_times)
            print(f"Deviazione standard: {std_dev_time:.4f} secondi")
        else:
            print("Deviazione standard non calcolabile con una sola esecuzione valida.")
    
        print(f"Tempi individuali registrati: {[round(t, 4) for t in execution_times]}")
    else:
        print("Nessuna esecuzione completata con successo, statistiche non disponibili.")