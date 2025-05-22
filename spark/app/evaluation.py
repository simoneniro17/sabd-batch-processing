import os
import statistics
import time
import inspect
import csv
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

class Evaluation:
    def __init__(self, spark, runs, output, query_id, query_type):
        self.execution_time = []    # Contiamo solo i tempi delle run che hanno avuto successo
        self.runs = runs+1
        self.query_type = query_type
        self.spark_session = spark
        self.output_path = output
        self.query_id = query_id

        print(f"Avvio misurazione prestazioni con {self.runs - 1} esecuzioni...")


    def run(self, func, *args, **kwargs):

        for i in range(self.runs):
            try:
                start = time.time()
                func(*args, **kwargs)
                elapsed = time.time() - start

                # Aggiungiamo il tempo solo se l'esecuzione è andata a buon fine
                self.execution_time.append(elapsed)
            except Exception as e:
                print(f"Errore durante l'esecuzione {i + 1}: {e}")


    def calculate_statistics(self):
        if not self.execution_time:
            return {}

        if len(self.execution_time) > 1:
            self.execution_time = self.execution_time[1:]  # Ignora il primo tempo se ci sono più esecuzioni
            
        stats = {
            "num_runs": len(self.execution_time),
            "time_avg": statistics.mean(self.execution_time),
            "time_min": min(self.execution_time),
            "time_max": max(self.execution_time),
        }
        
        # La deviazione standard è calcolabile solo se c'è più di un tempo di esecuzione
        if len(self.execution_time) > 1:
            std_dev = statistics.stdev(self.execution_time)
            stats["std_dev"] = std_dev

        return stats


    def evaluate(self):
        if not self.execution_time:
            print("Nessuna esecuzione completata con successo, statistiche non disponibili.")
            return
            
        stats = self.calculate_statistics()
        
        print(f"\n{'='*60}")
        print(f"STATISTICHE PRESTAZIONI")
        print(f"{'='*60}")
        print(f"Esecuzioni valide: {stats['num_runs']}")
        print(f"Tempo medio: {stats['time_avg']:.4f} secondi")
        print(f"Tempo minimo: {stats['time_min']:.4f} secondi")
        print(f"Tempo massimo: {stats['time_max']:.4f} secondi")

        if "std_dev" in stats:
            print(f"Deviazione standard: {stats['std_dev']:.4f} secondi")

        print(f"Tempi individuali: {[round(t, 4) for t in self.execution_time]}")
                
        print(f"{'='*60}")
        
        self.export_stats_to_hdfs()


    def export_stats_to_hdfs(self):
        # Estrai il nome dello script chiamante
        calling_file = inspect.getfile(inspect.stack()[2][0])
        filename = os.path.splitext(os.path.basename(calling_file))[0]

        # Estrai il nome base della query rimuovendo suffissi come "_sql"
        if "_" in filename:
            base_name, _ = filename.split("_", 1)
            query_id = base_name
        else:
            query_id = filename

        # Calcola le statistiche
        stats = self.calculate_statistics()
        if not stats:
            print("Nessuna statistica da esportare.")
            return

        # Prepara i dati in forma di lista di dizionari
        row = {
            "query-id": query_id,
            "query-type": self.query_type,
            "num-runs": stats["num_runs"],
            "time-avg (s)": round(stats["time_avg"], 4),
            "time-min (s)": round(stats["time_min"], 4),
            "time-max (s)": round(stats["time_max"], 4),
            "std-dev (s)": round(stats["std_dev"], 4) if "std_dev" in stats else None
        }

        data = [row]

        # Definisce lo schema del DataFrame
        schema = StructType([
            StructField("query-id", StringType(), True),
            StructField("query-type", StringType(), True),
            StructField("num-runs", IntegerType(), True),
            StructField("time-avg (s)", FloatType(), True),
            StructField("time-min (s)", FloatType(), True),
            StructField("time-max (s)", FloatType(), True),
            StructField("std-dev (s)", FloatType(), True),
        ])

        # Crea un DataFrame Spark
        df = self.spark_session.createDataFrame(data, schema)

        # Scrive su HDFS
        try:
            self.output_path = self.output_path.rstrip("/") + "/evaluation_" + self.query_id
            df.coalesce(1).write.mode("append").option("header", True).csv(self.output_path)
            print(f"Statistiche aggregate salvate su HDFS in {self.output_path}")
        except Exception as e:
            print(f"Errore durante il salvataggio delle statistiche su HDFS: {e}")