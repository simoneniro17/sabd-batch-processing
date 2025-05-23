from pyspark.sql import SparkSession
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.sql.functions import col, udf, abs, round
from pyspark.sql.types import DoubleType
import argparse
import os
import numpy as np

from evaluation import Evaluation


def find_optimal_k_silhouette(df, features_col, min_k=2, max_k=10):
    evaluator = ClusteringEvaluator(featuresCol=features_col, predictionCol='prediction', metricName='silhouette', distanceMeasure='squaredEuclidean')
    silhouette_scores = {}
    #print(f"\n--- Metodo Indice di Silhouette ---")
    print(f"Determinazione del k ottimale (Silhouette) tra {min_k} e {max_k}...")

    for k_val in range(min_k, max_k + 1):
        kmeans = KMeans(featuresCol=features_col, k=k_val, seed=30)
        model = kmeans.fit(df)
        predictions = model.transform(df)
        score = evaluator.evaluate(predictions)
        silhouette_scores[k_val] = score
        #print(f"  Punteggio Silhouette per k={k_val}: {score:.4f}")

    if not silhouette_scores:
        print("Nessun punteggio Silhouette calcolato.")
        return None
        
    optimal_k = max(silhouette_scores, key=silhouette_scores.get)
    print(f"K ottimale (Silhouette) selezionato: {optimal_k} con punteggio: {silhouette_scores[optimal_k]:.4f}")
    return optimal_k


def process_clustering(spark, input_path):
    # Carichiamo i dati da file CSV
    df = spark.read.csv(input_path, header=True, inferSchema=False)

    # Prima di procedere, verifichiamo che le colonne richieste siano presenti
    required_cols = ["Country", "Carbon intensity gCO₂eq/kWh (direct)"]
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        raise ValueError(f"Colonne mancanti nel dataset: {missing}")

    # Selezioniamo solo le colonne necessarie e rinominiamo per chiarezza
    df = df.select(
        col("Country"),
        (col("Carbon intensity gCO₂eq/kWh (direct)").cast("double")).alias("carbon-intensity")
    )
            
    # Assembliamo le feature in un vettore
    assembler = VectorAssembler(inputCols=["carbon-intensity"], outputCol="features_raw")
    df_assembled = assembler.transform(df)

    # K-means calcola le distanze tra i punti dati per formare i cluster. Se le feature hanno scale molto diverse
    # (e.g. una feature varia da 0 a 1, un'altra da 0 a 10000), la feature con la scala più grande dominerà il calcolo della distanza.
    # Questo può portare a cluster che riflettono principalmente la varianza di quella singola feature, ignorando le altre.
    # Per evitare questo, è buona norma scalare le feature in modo che abbiano media 0 e varianza 1.
    scaler = StandardScaler(inputCol="features_raw", outputCol="features", withStd=True, withMean=False)
    scaler_model = scaler.fit(df_assembled)
    df_scaled = scaler_model.transform(df_assembled)
    
    df_scaled = df_scaled.select("country", "carbon-intensity", "features")

    # Determiniamo il k ottimale
    num_distinct_points = df_scaled.select("carbon-intensity").distinct().count()
    if num_distinct_points < 2:
        print(f"Numero di punti dati distinti ({num_distinct_points}) insufficiente per eseguire il clustering. Ne servono almeno 2.")
        return
        
    # Il valore massimo di k non dovrebbe superare il numero di punti distinti, non avrebbe senso avere più cluster che punti
    max_k_limit = min(30, num_distinct_points)
    k_silhouette = find_optimal_k_silhouette(df_scaled, features_col="features", min_k=2, max_k=max_k_limit)

    # Impostiamo il k finale per il clustering
    final_k = None
    if k_silhouette is not None:
        final_k = k_silhouette
        print(f"Utilizzo k={final_k} (da Silhouette) per il clustering finale.")
    else:
        final_k = 3 # Default se entrambi i metodi falliscono
        print(f"Nessun k ottimale determinato --> valore di default k={final_k}.")
    
    # Modello K-means con il k finale
    print(f"\nAddestramento del modello K-means finale con k={final_k}...")
    kmeans = KMeans(featuresCol="features", k=final_k, seed=30)
    model = kmeans.fit(df_scaled)

    # Facciamo le predizioni (assegniamo i cluster)
    predictions = model.transform(df_scaled)
    
    # Per ottenere i centri nelle coordinate originali, non scalate, invertiamo la trasformazione dello scaler
    # Dato che abbiamo scalato solo con std (withMean=False), possiamo "invertire" moltiplicando per lo std 
    centers_scaled = model.clusterCenters()
    std_values = scaler_model.std       

    # Dizionario per memorizzare i centri dei cluster
    cluster_centers = {}
    for i, center in enumerate(centers_scaled):
        original_center_value = float(center[0] * std_values[0])  # Solo una feature, quindi solo il primo elemento
        cluster_centers[i] = original_center_value
        #print(f"  Cluster {i}: [{original_center_value}]")

    def get_cluster_center(cluster_id):
        return cluster_centers.get(cluster_id, 0.0)
    
    cluster_center_udf = udf(get_cluster_center, DoubleType())

    output_df = predictions.select(
        col("country"),
        col("carbon-intensity"),
        col("prediction").alias("cluster_id")
    )

    # Aggiungiamo il centro del cluster e la distanza dal centro
    output_df = output_df.withColumn("cluster_center",
                                     round(cluster_center_udf(col("cluster_id")), 2))
    
    # Calcoliamo la distanza dal centro del cluster
    output_df = output_df.withColumn("distance_from_center",
                                     round(abs(col("carbon-intensity") - col("cluster_center")), 2))
    
    # Ordiniamo per ID del cluster e poi per paese
    output_df = output_df.orderBy("cluster_id", "country")
    
    return output_df


def main_query4(spark, input_file, output_path):

        # Processiamo i dati e applichiamo il clustering
        result_df = process_clustering(spark, input_file)

        # Salviamo i risultati in formato CSV su HDFS
        result_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Query 4: Clustering K-means sui dati di intensità di carbonio.")
    parser.add_argument("--input", required=True, help="Percorso del file CSV di input su HDFS")
    parser.add_argument("--output", required=True, help="Cartella di output su HDFS per i risultati")
    parser.add_argument("--runs", type=int, default=1, help="Numero di esecuzioni per la misurazione delle prestazioni")

    args = parser.parse_args()

    # Costruzione dei percorsi HDFS
    HDFS_BASE = os.getenv("HDFS_BASE")
    input = f"{HDFS_BASE.rstrip('/')}/{args.input.lstrip('/')}"
    output = f"{HDFS_BASE.rstrip('/')}/{args.output.lstrip('/')}"

    # Inizializziamo la sessione Spark
    spark = SparkSession.builder.appName("Query4-Clustering-KMeans").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR") 

    try:
        # Istanziazione classe per la valutazione delle prestazioni
        evaluator = Evaluation(spark, args.runs, output, "query4", "DF")

        # Esecuzione e valutazione
        evaluator.run(main_query4,spark, input, output) 
        evaluator.evaluate()
    except Exception as e:
        print(f"Errore durante l'elaborazione di Query 4: {e}")
        raise # Rilanciamo l'eccezione affinché le statistiche vengano calcolate solo se la query ha successo
    finally:
        spark.stop()