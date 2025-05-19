import time
import redis
from hdfs import InsecureClient
from requests.exceptions import ConnectionError
import os
import sys

# Attendi che il NameNode sia pronto
namenode_url = os.getenv('NAMENODE')
while True:
    try:
        hdfs_client = InsecureClient(namenode_url, user='root')
        hdfs_client.status('/')
        print("✔️ NameNode disponibile.")
        break
    except ConnectionError:
        print("⏳ In attesa del NameNode...")
        time.sleep(3)

# Attendi che Redis sia pronto
while True:
    try:
        r = redis.Redis(host='redis', port=6379)
        r.ping()
        print("✔️ Redis disponibile.")
        break
    except redis.exceptions.ConnectionError:
        print("⏳ In attesa di Redis...")
        time.sleep(3)

# Funzione ricorsiva per visitare tutte le directory
def visit_and_store(hdfs_client, redis_client, current_path, base_path):
    try:
        items = hdfs_client.list(current_path, status=True)
        for name, info in items:
            full_path = os.path.join(current_path, name)

            if info['type'] == 'DIRECTORY':
                visit_and_store(hdfs_client, redis_client, full_path, base_path)

            elif info['type'] == 'FILE' and name.endswith('.csv'):
                try:
                    with hdfs_client.read(full_path, encoding='utf-8') as reader:
                        content = reader.read()

                        # Ottieni il nome della directory madre
                        parent_dir = os.path.basename(os.path.dirname(full_path))
                        redis_key = f"hdfs_data:{parent_dir}.csv"

                        redis_client.set(redis_key, content)
                        print(f"✔️ File CSV salvato in Redis con chiave: {redis_key}")

                except Exception as e:
                    print(f"❌ Errore nella lettura del file {full_path}: {e}")
            else:
                print(f"ℹ️ Ignorato file non CSV: {full_path}")
    except Exception as e:
        print(f"❌ Errore durante l'accesso a {current_path}: {e}")


# Percorso iniziale da cui partire
if len(sys.argv) < 2:
    print("❌ Errore: specificare il path HDFS come argomento.")
    sys.exit(1)

base_hdfs_path = sys.argv[1]

# Avvia la lettura ricorsiva e copia su Redis
visit_and_store(hdfs_client, r, base_hdfs_path, base_hdfs_path)
