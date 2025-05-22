import time
import redis
from hdfs import InsecureClient
from requests.exceptions import ConnectionError
import os
import argparse


# Verifica se HDFS è disponibile
def get_hdfs(namenode_url):
    while True:
        try:
            hdfs_client = InsecureClient(namenode_url, user='root')
            hdfs_client.status('/')
            break
        except ConnectionError:
            print("In attesa di HDFS...")
            time.sleep(3)
    return hdfs_client


# Verifica se Redis è disponibile
def get_redis():
    while True:
        try:
            r = redis.Redis(host='redis', port=6379)
            r.ping()
            break
        except redis.exceptions.ConnectionError:
            print("In attesa di Redis...")
            time.sleep(3)
    return r


# Funzione ricorsiva per visitare tutte le directory
def store(hdfs_client, redis_client, current_path, base_path):
    try:
        # Lista file e directory in HDFS nella directory corrente
        items = hdfs_client.list(current_path, status=True)

        for name, info in items:
            full_path = os.path.join(current_path, name)    # Percorso completo del file/directory

            # Se è una directory, richiamiamo ricorsivamente la funzione
            if info['type'] == 'DIRECTORY':
                store(hdfs_client, redis_client, full_path, base_path)

            # Se è un file e termina con .csv, lo leggiamo e lo salviamo in Redis
            elif info['type'] == 'FILE' and name.endswith('.csv'):
                try:
                    with hdfs_client.read(full_path, encoding='utf-8') as reader:
                        content = reader.read()

                        parent_dir = os.path.basename(os.path.dirname(full_path))
                        redis_key = f"hdfs_data:{parent_dir}.csv"

                        redis_client.set(redis_key, content)
                except Exception as e:
                    print(f"Errore nella lettura del file CSV {full_path}: {e}")
    except Exception as e:
        print(f"Errore durante l'accesso a {current_path}: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Carica i file da HDFS a Redis.")
    parser.add_argument("--directory", required=True, help="Percorso della cartella in cui salvare i dati (es. /results/)")
    
    args = parser.parse_args()

    namenode_url = os.getenv('NAMENODE')
    hdfs_client = get_hdfs(namenode_url)
    redis_client = get_redis()

    base_hdfs_path = args.directory

    store(hdfs_client, redis_client, base_hdfs_path, base_hdfs_path)