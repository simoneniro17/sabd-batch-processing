import os
import statistics
import time
import inspect
import csv
class Evaluation:
    def __init__(self, runs):
        self.execution_time = []
        self.runs = runs
        print(f"Avvio misurazione prestazioni con {self.runs} esecuzioni...")

    def run(self, func, *args, **kwargs):
        """Esegue la funzione `func` e salva il tempo di esecuzione."""
        for i in range(self.runs):
            try:
                start = time.time()
                func(*args, **kwargs)
                elapsed = time.time() - start
                self.execution_time.append(elapsed)
            except Exception as e:
                print(f"Errore durante l'esecuzione {i + 1}: {e}")

    def evaluate(self):
        if self.execution_time:
            mean_time = statistics.mean(self.execution_time)
            print(f"\nStatistiche prestazioni dopo {len(self.execution_time)} esecuzioni valide:")
            print(f"Tempo medio di esecuzione: {mean_time:.4f} secondi")

            if len(self.execution_time) > 1:
                std_dev_time = statistics.stdev(self.execution_time)
                print(f"Deviazione standard: {std_dev_time:.4f} secondi")
            else:
                print("Deviazione standard non calcolabile con una sola esecuzione valida.")
            
            print(f"Tempi individuali registrati: {[round(t, 4) for t in self.execution_time]}")
            self.export_stats_to_csv()
        else:
            print("Nessuna esecuzione completata con successo, statistiche non disponibili.")

    def export_stats_to_csv(self, output_dir: str = "./evaluation_results"):
        os.makedirs(output_dir, exist_ok=True)
        # prende il nome dallo script chiamante → es. query1.py → query1.csv
        calling_file = inspect.getfile(inspect.stack()[2][0])
        query_name = os.path.splitext(os.path.basename(calling_file))[0]
        file_path = os.path.join(output_dir, f"{query_name}.csv")
        try:
            with open(file_path, mode="w", newline="") as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(["run", "tempo_esecuzione_sec"])
                for i, t in enumerate(self.execution_time, start=1):
                    writer.writerow([i, round(t, 4)])

            print(f"Statistiche salvate in {file_path}")
        except Exception as e:
            print(f"Errore durante il salvataggio del CSV: {e}")