import time
import redis
from hdfs import InsecureClient
from requests.exceptions import ConnectionError

# Attendi che il NameNode sia pronto
namenode_url = 'http://namenode:9870'
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

# Lettura da HDFS
file_path = '/data/input1.txt'  # Path corretto in HDFS
try:
    with hdfs_client.read(file_path, encoding='utf-8') as reader:
        content = reader.read()
        print(f"✔️ File letto da HDFS: {file_path}")
except Exception as e:
    print(f"❌ Errore nella lettura del file HDFS: {e}")
    content = None

# Scrittura su Redis
if content:
    r.set('hdfs_data', content)
    print("✔️ Dati scritti su Redis.")

    # Estrazione dei dati da Redis
    redis_data = r.get('hdfs_data')
    if redis_data:
        print("✔️ Dati letti da Redis:")
        print(redis_data.decode('utf-8'))  # Decodifica e stampa il contenuto
    else:
        print("❌ La chiave 'hdfs_data' non esiste su Redis.")
