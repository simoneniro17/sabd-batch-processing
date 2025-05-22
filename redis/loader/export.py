import os
import redis
import argparse

# Funzione per esportare i dati da Redis nella cartella "results" in locale
def export_from_redis(output_dir):
    
    # Connessione a Redis
    r = redis.Redis(host="redis", port=6379, decode_responses=True)
    
    # Recuperiamo le chiavi che iniziano con "hdfs_data:" (avremo tante chiavi quanti sono i risultati csv)
    keys = r.keys("hdfs_data:*")

    os.makedirs(output_dir, exist_ok=True)
    for key in keys:

        if key.startswith("hdfs_data:"):
            key_no_prefix = key[len("hdfs_data:"):] # Rimuoviamo il prefisso "hdfs_data:" se presente
        else:
            key_no_prefix = key

        filename = os.path.join(output_dir, key_no_prefix)  # Creiamo il percorso completo del file

        # Prendiamo i dati da Redis per la chiave corrente
        data = r.get(key)
        
        # Se il file esiste già, lo sovrascriviamo
        with open(filename, "w", encoding="utf-8") as f:
            f.write(data)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Esporta e salva i dati da Redis.")
    parser.add_argument("--directory", required=True, help="Percorso della cartella in cui si vogliono esportare i dati (e.g. ./results).")

    args = parser.parse_args()

    export_from_redis(args.directory)