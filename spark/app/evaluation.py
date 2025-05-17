import statistics
import time

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
        else:
            print("Nessuna esecuzione completata con successo, statistiche non disponibili.")
