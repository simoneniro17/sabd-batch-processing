from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
import numpy as np
import argparse
import time
import statistics

def process_data(spark, input_path):
    """Process the yearly data CSV"""
    # Read CSV file
    df = spark.read.csv(input_path, header=True)
    
    # Select only needed columns and rename for clarity
    return df.select(
        col("Zone id").alias("country"),
        col("Carbon intensity gCO₂eq/kWh (direct)").cast("float").alias("carbon_intensity")
    )

def find_optimal_k(data, max_k=30):
    """Find optimal k using elbow method"""
    assembler = VectorAssembler(
        inputCols=["carbon_intensity"],
        outputCol="features"
    )
    vector_data = assembler.transform(data)
    
    # Calculate WCSS for different k values
    wcss = []
    for k in range(2, max_k + 1):
        kmeans = KMeans().setK(k).setSeed(42)
        model = kmeans.fit(vector_data)
        wcss.append(model.summary.trainingCost)
    
    # Find elbow point using the maximum curvature
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
    """Perform k-means clustering"""
    assembler = VectorAssembler(
        inputCols=["carbon_intensity"],
        outputCol="features"
    )
    vector_data = assembler.transform(data)
    
    kmeans = KMeans().setK(k).setSeed(42)
    model = kmeans.fit(vector_data)
    
    # Add predictions and sort by carbon intensity for better visualization
    predictions = model.transform(vector_data)
    return predictions.select(
        "country", 
        "carbon_intensity", 
        "prediction"
    ).orderBy("carbon_intensity")

def main(input_path, output_path):
    spark = SparkSession.builder \
        .appName("Q4-Clustering") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    execution_time = 0
    try:
        start_time = time.time()
        
        # Process the yearly data
        data = process_data(spark, input_path)
        
        # Find optimal k using elbow method
        optimal_k = find_optimal_k(data)
        print(f"Optimal number of clusters (k) found: {optimal_k}")
        
        # Perform clustering with optimal k
        results = perform_clustering(data, optimal_k)
        
        # Save clustering results
        results.coalesce(1).write.mode("overwrite") \
               .option("header", True).csv(output_path)
        
        end_time = time.time()
        execution_time = end_time - start_time
        
    except Exception as e:
        print(f"Error during processing: {e}")
    finally:
        spark.stop()
    return execution_time

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Query 4: Country clustering by carbon intensity")
    parser.add_argument("--input", required=True, help="Path to 2024 yearly CSV file")
    parser.add_argument("--output", required=True, help="Output directory for results")
    parser.add_argument("--runs", type=int, default=1, help="Number of execution runs")
    
    args = parser.parse_args()
    
    if args.runs < 1:
        print("Number of runs must be at least 1")
        exit(1)
    
    execution_times = []
    print(f"Starting performance measurement for Query4 with {args.runs} runs...")
    
    for i in range(args.runs):
        run_time = main(args.input, args.output)
        if run_time > 0:
            execution_times.append(run_time)
            print(f"Run {i+1} completed in {run_time:.4f} seconds")
        else:
            print(f"Run {i+1} failed")
    
    if execution_times:
        mean_time = statistics.mean(execution_times)
        print(f"\nPerformance statistics after {len(execution_times)} valid runs:")
        print(f"Mean execution time: {mean_time:.4f} seconds")
        
        if len(execution_times) > 1:
            std_dev = statistics.stdev(execution_times)
            print(f"Standard deviation: {std_dev:.4f} seconds")
        
        print(f"Individual run times: {[round(t, 4) for t in execution_times]}")
    else:
        print("No successful runs completed")