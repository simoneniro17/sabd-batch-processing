from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, col, hour, date_format, avg, min, max, expr, lit, percentile_approx

from pyspark.sql.window import Window
import argparse
from evaluation import Evaluation
import os

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

    try:
        it_results = process_file(spark, input_it, "IT")
        se_results = process_file(spark, input_se, "SE")

        combined_df = it_results.unionByName(se_results)
        combined_df.coalesce(1).write.mode("overwrite").option("header", True).csv(output_path)
    except Exception as e:
        print(f"Errore durante l'elaborazione: {e}")
    finally:
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Query 3: elaborazione dati per IT e SE.")
    parser.add_argument("--input_it", required=True)
    parser.add_argument("--input_se", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--runs", type=int, default=1)

    args = parser.parse_args()

    HDFS_BASE = os.getenv("HDFS_BASE")
    input_it = f"{HDFS_BASE.rstrip('/')}/{args.input_it.lstrip('/')}"
    input_se = f"{HDFS_BASE.rstrip('/')}/{args.input_se.lstrip('/')}"
    output = f"{HDFS_BASE.rstrip('/')}/{args.output.lstrip('/')}"

    evaluator = Evaluation(args.runs)
    evaluator.run(main, input_it, input_se, output)
    evaluator.evaluate()
