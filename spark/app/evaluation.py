import os
import statistics
import time
import inspect
import csv


class Evaluation:
    def __init__(self, runs, query_type="DataFrame"):
        self.execution_time = []    # Contiamo solo i tempi delle run che hanno avuto successo
        self.runs = runs
        self.query_type = query_type
    
        print(f"Avvio misurazione prestazioni con {self.runs} esecuzioni...")


    def run(self, func, *args, **kwargs):
        """Esegue la funzione `func` e salva il tempo di esecuzione."""
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
        """Calcola le statistiche sui tempi di esecuzione."""
        if not self.execution_time:
            return {}
            
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
        """Valuta e stampa le statistiche delle prestazioni."""
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
        
        self.export_stats_to_csv()


    def export_stats_to_csv(self, output_dir = "./eval_results"):
        """Esporta le statistiche aggregate in un unico file CSV."""
        os.makedirs(output_dir, exist_ok=True)
        
        # Nome della query dallo script chiamante
        calling_file = inspect.getfile(inspect.stack()[2][0])
        filename = os.path.splitext(os.path.basename(calling_file))[0]

        # Estrai il nome base della query rimuovendo suffissi come "_sql"
        if "_" in filename:
            base_name, _ = filename.split("_", 1)  # Divide alla prima occorrenza di "_"
            query_id = base_name
        else:
            query_id = filename
        
        # File per le statistiche aggregate
        aggregate_file = os.path.join(output_dir, "performance.csv")
        
        # Calcola le statistiche
        stats = self.calculate_statistics()
        if not stats:
            print("Nessuna statistica da esportare.")
            return
            
        # Prepara la riga per il file aggregato
        row = {
            "query-id": query_id,
            "query-type": self.query_type,
            "num-runs": stats["num_runs"],
            "time-avg (s)": round(stats["time_avg"], 4),
            "time-min (s)": round(stats["time_min"], 4),
            "time-max (s)": round(stats["time_max"], 4),
        }

        if "std_dev" in stats:
            row["std-dev (s)"] = round(stats["std_dev"], 4)
        else:
            row["std-dev (s)"] = "N/A"

        # Verifica se il file aggregato esiste già
        file_exists = os.path.isfile(aggregate_file)
        
        # Scrive nel file aggregato
        try:
            with open(aggregate_file, mode="a", newline="") as csvfile:
                fieldnames = ["query-id", "query-type", "num-runs", "time-avg (s)", 
                             "time-min (s)", "time-max (s)", "std-dev (s)"]
                
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                if not file_exists:
                    writer.writeheader()
                writer.writerow(row)
                
            print(f"Statistiche aggregate salvate in {aggregate_file}")
        except Exception as e:
            print(f"Errore durante il salvataggio delle statistiche aggregate: {e}")