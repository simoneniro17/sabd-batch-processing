from pyspark.sql import SparkSession
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.sql.functions import col
import argparse
import os
import numpy as np

from evaluation import Evaluation

def find_optimal_k_silhouette(df, features_col, min_k=2, max_k=10):
    evaluator = ClusteringEvaluator(featuresCol=features_col, predictionCol='prediction', metricName='silhouette')
    silhouette_scores = {}
    print(f"\n--- Metodo Indice di Silhouette ---")
    print(f"Determinazione del k ottimale (Silhouette) tra {min_k} e {max_k}...")

    for k_val in range(min_k, max_k + 1):
        kmeans = KMeans(featuresCol=features_col, k=k_val, seed=42)
        model = kmeans.fit(df)
        predictions = model.transform(df)
        score = evaluator.evaluate(predictions)
        silhouette_scores[k_val] = score
        print(f"  Punteggio Silhouette per k={k_val}: {score:.4f}")

    if not silhouette_scores:
        print("Nessun punteggio Silhouette calcolato.")
        return None
        
    optimal_k = max(silhouette_scores, key=silhouette_scores.get)
    print(f"K ottimale (Silhouette) selezionato: {optimal_k} con punteggio: {silhouette_scores[optimal_k]:.4f}")
    return optimal_k

def find_optimal_k_elbow(df, features_col, min_k=2, max_k=10):
    print(f"\n--- Metodo del Gomito (Elbow Method) ---")
    print(f"Calcolo WSSSE per k tra {min_k} e {max_k}...") # (Within Set Sum of Squared Errors)
    wssse_values = {}
    
    for k_val in range(min_k, max_k + 1):
        kmeans = KMeans(featuresCol=features_col, k=k_val, seed=42)
        model = kmeans.fit(df)

        try:
            wssse = model.summary.trainingCost
        except AttributeError:
            wssse = model.computeCost(df) 
            
        wssse_values[k_val] = wssse
        print(f"  WSSSE per k={k_val}: {wssse:.2f}")

    if not wssse_values:
        print("Nessun valore WSSSE calcolato.")
        return None

    # Identificare il gomito è "soggettivo". Stampiamo i valori per analizzarli.
    print("Valori WSSSE (k: WSSSE):")
    for k_v, w_v in wssse_values.items():
        print(f"  k={k_v}: {w_v:.2f}")
    
    # Tentativo di suggerire un k basato sulla "seconda derivata" (cambiamento nel tasso di diminuzione)
    # Approccio euristico e potrebbe non essere sempre perfetto
    if len(wssse_values) >= 3:
        # Calcola la diminuzione percentuale
        decreases = {}
        sorted_k = sorted(wssse_values.keys())
        for i in range(len(sorted_k) - 1):
            k1, k2 = sorted_k[i], sorted_k[i+1]
            decrease = wssse_values[k1] - wssse_values[k2]
            decreases[k2] = decrease
        
        # Calcola il cambiamento nella diminuzione (approssimazione della seconda derivata)
        decrease_changes = {}
        sorted_decrease_k = sorted(decreases.keys())
        for i in range(len(sorted_decrease_k) -1):
            k1, k2 = sorted_decrease_k[i], sorted_decrease_k[i+1]
            change = decreases[k1] - decreases[k2] # Vogliamo un grande calo qui
            decrease_changes[k2] = change

        if decrease_changes:
            # Troviamo il k che massimizza la "distanza dal segmento" che unisce il primo e l'ultimo punto.
            # Questo è più robusto per la forma a "gomito".
            points = np.array([[k, wssse_values[k]] for k in sorted(wssse_values.keys())])
            if len(points) > 1:
                line_vec = points[-1] - points[0]
                line_vec_norm = line_vec / np.sqrt(np.sum(line_vec**2))
                vec_from_first = points - points[0]
                scalar_product = np.sum(vec_from_first * np.tile(line_vec_norm, (len(points), 1)), axis=1)
                vec_from_first_parallel = np.outer(scalar_product, line_vec_norm)
                vec_to_line = vec_from_first - vec_from_first_parallel
                dist_to_line = np.sqrt(np.sum(vec_to_line ** 2, axis=1))
                
                if len(dist_to_line) > 0:
                    elbow_index = np.argmax(dist_to_line)
                    optimal_k_elbow = sorted(wssse_values.keys())[elbow_index]
                    print(f"K ottimale (Gomito - euristico) suggerito: {optimal_k_elbow}")
                    return optimal_k_elbow
    
    print("Impossibile suggerire un k ottimale dal metodo del gomito in modo programmatico con i dati attuali.")
    return None


def process_clustering(spark, input_path, output_path):
    try:
        df = spark.read.csv(input_path, header=True, inferSchema=False)

        df = df.select(
            col("Country"),
            (col("Carbon intensity gCO₂eq/kWh (direct)").cast("float")).alias("carbon-intensity")
        )
                
        # Assembla le feature in una singola colonna vettoriale
        assembler = VectorAssembler(inputCols=["carbon-intensity"], outputCol="features_raw")
        df_assembled = assembler.transform(df)

        # K-means calcola le distanze tra i punti dati per formare i cluster. Se le feature hanno scale molto diverse
        # (e.g. una feature varia da 0 a 1, un'altra da 0 a 10000), la feature con la scala più grande dominerà il calcolo della distanza.
        # Questo può portare a cluster che riflettono principalmente la varianza di quella singola feature, ignorando le altre.
        # Per evitare questo, è buona norma scalare le feature in modo che abbiano media 0 e varianza 1.
        scaler = StandardScaler(inputCol="features_raw", outputCol="features", withStd=True, withMean=False)
        scaler_model = scaler.fit(df_assembled)
        df_scaled = scaler_model.transform(df_assembled)
        
        df_scaled.select("country", "carbon-intensity", "features")

        # Determina il k ottimale
        num_distinct_points = df_scaled.select("carbon-intensity").distinct().count()
        if num_distinct_points < 2:
            print(f"Numero di punti dati distinti ({num_distinct_points}) insufficiente per eseguire il clustering. Ne servono almeno 2.")
            return
            
        # max_k non dovrebbe superare il numero di punti distinti
        max_k_limit = min(30, num_distinct_points)
        if max_k_limit < 2:
            print(f"Max k ({max_k_limit}) è troppo basso per determinare k ottimale. Servono almeno 2 punti distinti.")
            if num_distinct_points >= 2:
                k_silhouette = 2
                k_elbow = 2
                print("Impostazione predefinita di k a 2 a causa di dati limitati.")
            else:
                print("Impossibile procedere con il clustering.")
                return
        else:
            k_silhouette = find_optimal_k_silhouette(df_scaled, features_col="features", min_k=2, max_k=max_k_limit)
            k_elbow = find_optimal_k_elbow(df_scaled, features_col="features", min_k=2, max_k=max_k_limit)

        print(f"\n--- Riepilogo Determinazione k Ottimale ---")
        print(f"K ottimale suggerito da Indice di Silhouette: {k_silhouette}")
        print(f"K ottimale suggerito da Metodo del Gomito (euristico): {k_elbow}")

        # Si potrebbe usare una logica specifica, per ora usiamo Silhouette se disponibile, altrimenti Elbow, o un default.
        final_k = None
        if k_silhouette is not None:
            final_k = k_silhouette
            print(f"Utilizzo k={final_k} (da Silhouette) per il clustering finale.")
        elif k_elbow is not None:
            final_k = k_elbow
            print(f"Utilizzo k={final_k} (da Gomito) per il clustering finale.")
        else:
            final_k = 3 # Default se entrambi i metodi falliscono
            print(f"Nessun k ottimale determinato --> valore di default k={final_k}.")
        
        # Modello K-means con il k finale
        print(f"\nAddestramento del modello K-means finale con k={final_k}...")
        kmeans = KMeans(featuresCol="features", k=final_k, seed=42)
        model = kmeans.fit(df_scaled)

        # Facciamo le predizioni (assegniamo i cluster)
        predictions = model.transform(df_scaled)
        
        print("\nCentri dei cluster (valori scalati):")
        centers_scaled = model.clusterCenters()
        for i, center in enumerate(centers_scaled):
            print(f"  Cluster {i}: {center}")
        
        # Per ottenere i centri dei cluster nelle coordinate originali (non scalate)
        # è necessario invertire la trasformazione dello scaler.
        # Questo è più complesso e dipende da come è stato fatto lo scaling (con media/std).
        # Dato che abbiamo scalato solo con std (withMean=False), possiamo "invertire" moltiplicando per lo std.
        # Tuttavia, StandardScalerModel non espone direttamente gli std usati per ogni feature in modo semplice.
        # Per semplicità, mostriamo i centri scalati. L'interpretazione si basa sui dati scalati.

        output_df = predictions.select(
            col("country"),
            col("carbon-intensity"),
            col("prediction").alias("cluster_id")
        ).orderBy("cluster_id", "country")
        
        print("\nRisultati del Clustering:")
        output_df.show(truncate=False)

        output_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)
    except Exception as e:
        print(f"Errore durante il processo di clustering: {e}")
    finally:
        spark.stop()


def main_spark_job(input_file, output_dir):
    spark = SparkSession.builder.appName("Query4-Clustering-KMeans").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR") 

    print(f"Inizio elaborazione Query 4")
    process_clustering(spark, input_file, output_dir)
    print(f"Elaborazione Query 4 completata. Output in: {output_dir}")

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Query 4: Clustering K-means sui dati di intensità di carbonio.")
    parser.add_argument("--input", required=True, help="Percorso del file CSV di input su HDFS")
    parser.add_argument("--output", required=True, help="Cartella di output su HDFS per i risultati")
    parser.add_argument("--runs", type=int, default=1, help="Numero di esecuzioni per la misurazione delle prestazioni")

    args = parser.parse_args()

    HDFS_BASE = os.getenv("HDFS_BASE")
    input = f"{HDFS_BASE.rstrip('/')}/{args.input.lstrip('/')}"
    output = f"{HDFS_BASE.rstrip('/')}/{args.output.lstrip('/')}"
    
    evaluator = Evaluation(args.runs)
    evaluator.run(main_spark_job, input, output) 
    evaluator.evaluate()