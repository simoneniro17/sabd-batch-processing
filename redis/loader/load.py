import time
import redis
from hdfs import InsecureClient
from requests.exceptions import ConnectionError
import os
import argparse

# TODO: si possono eliminare alcuni print


def get_hdfs(namenode_url):
    while True:
        try:
            hdfs_client = InsecureClient(namenode_url, user='root')
            hdfs_client.status('/')
            print("HDFS disponibile.")
            break
        except ConnectionError:
            print("In attesa di HDFS...")
            time.sleep(3)
    return hdfs_client

def get_redis():
    while True:
        try:
            r = redis.Redis(host='redis', port=6379)
            r.ping()
            print("Redis disponibile.")
            break
        except redis.exceptions.ConnectionError:
            print("In attesa di Redis...")
            time.sleep(3)
    return r

# Funzione ricorsiva per visitare tutte le directory
def store(hdfs_client, redis_client, current_path, base_path):
    try:
        items = hdfs_client.list(current_path, status=True)
        for name, info in items:
            full_path = os.path.join(current_path, name)

            if info['type'] == 'DIRECTORY':
                store(hdfs_client, redis_client, full_path, base_path)

            elif info['type'] == 'FILE' and name.endswith('.csv'):
                try:
                    with hdfs_client.read(full_path, encoding='utf-8') as reader:
                        content = reader.read()

                        # Ottieni il nome della directory madre
                        parent_dir = os.path.basename(os.path.dirname(full_path))
                        redis_key = f"hdfs_data:{parent_dir}.csv"

                        redis_client.set(redis_key, content)
                        print(f"File CSV salvato in Redis con chiave: {redis_key}")

                except Exception as e:
                    print(f"Errore nella lettura del file {full_path}: {e}")
            else:
                print(f"Ignorato file non CSV: {full_path}")
    except Exception as e:
        print(f"Errore durante l'accesso a {current_path}: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Carica i file da HDFS a Redis.")
    parser.add_argument("--directory", required=True, help="Percorso della cartella da salvare (es. /results/)")
    args = parser.parse_args()

    namenode_url = os.getenv('NAMENODE')
    hdfs_client = get_hdfs(namenode_url)
    redis_client = get_redis()

    base_hdfs_path = args.directory

    store(hdfs_client, redis_client, base_hdfs_path, base_hdfs_path)