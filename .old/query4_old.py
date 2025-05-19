from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
import numpy as np
import argparse
import os

from evaluation import Evaluation

def process_data(spark, input_path):
    df = spark.read.csv(input_path, header=True)
    
    # Rinominiamo la colonna per comodità
    return df.select(
        col("Country").alias("country"),
        (col("Carbon intensity gCO₂eq/kWh (direct)").cast("float")).alias("carbon_intensity")
    )

def find_optimal_k(data, max_k=30):
    assembler = VectorAssembler(
        inputCols=["carbon_intensity"],
        outputCol="features"
    )
    vector_data = assembler.transform(data)
    
    # Calcoliamo il WCSS (Within-Cluster Sum of Squares) per diversi valori di k
    wcss = []
    for k in range(2, max_k + 1):
        kmeans = KMeans().setK(k).setSeed(42)
        model = kmeans.fit(vector_data)
        wcss.append(model.summary.trainingCost)
    
    # Troviamo il punto di "gomito" (elbow point)
    diffs = np.diff(wcss, 2)
    elbow_point = np.argmax(diffs) + 2
    
    # questo non va ovviamente graficato in spark ma è un'idea per grafana 
    # # Generate elbow curve plot
    # plt.figure(figsize=(10, 6))
    # plt.plot(range(1, max_k + 1), wcss, 'bx-')
    # plt.xlabel('Number of Clusters (k)')
    # plt.ylabel('Within-Cluster Sum of Squares (WCSS)')
    # plt.title('Elbow Method for Optimal k Selection')
    # plt.axvline(x=elbow_point, color='r', linestyle='--', 
    #             label=f'Elbow point (k={elbow_point})')
    # plt.legend()
    # plt.savefig('/app/elbow_curve.png')
    # plt.close()
    
    return elbow_point

def perform_clustering(data, k):
    assembler = VectorAssembler(
        inputCols=["carbon_intensity"],
        outputCol="features"
    )
    vector_data = assembler.transform(data)
    
    kmeans = KMeans().setK(k).setSeed(42)
    model = kmeans.fit(vector_data)
    
    predictions = model.transform(vector_data)
    return predictions.select(
        "country", 
        "carbon_intensity", 
        "prediction"
    ).orderBy("carbon_intensity")


def main(input_path, output_path):
    spark = SparkSession.builder.appName("Q4-Clustering").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    try:    
        # Processiamo i dati annuali
        data = process_data(spark, input_path)
        
        # Troviamo il valore ottimale di k usando l'elbow method
        optimal_k = find_optimal_k(data)
        print(f"Numero ottimale di cluster (k): {optimal_k}")
        
        # Eseguiamo il clustering con k ottimale
        results = perform_clustering(data, optimal_k)
        
        results.coalesce(1).write.mode("overwrite").option("header", True).csv(output_path)
    except Exception as e:
        print(f"Errore durante l'esecuzione: {e}")
    finally:
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Query 4: Analisi di clustering sui dati annuali relativi all'intensità di carbonio")
    parser.add_argument("--input", required=True, help="Path di input del file CSV")
    parser.add_argument("--output", required=True, help="Path di output per i risultati del clustering")
    parser.add_argument("--runs", type=int, default=1, help="Numero di esecuzioni")
    
    args = parser.parse_args()

    HDFS_BASE = os.getenv("HDFS_BASE")
    input = f"{HDFS_BASE.rstrip('/')}/{args.input.lstrip('/')}"
    output = f"{HDFS_BASE.rstrip('/')}/{args.output.lstrip('/')}"
    
    evaluator = Evaluation(args.runs)
    evaluator.run(main, input, output)
    evaluator.evaluate()