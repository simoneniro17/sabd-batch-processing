import subprocess

CONTAINER_NAME = "redis-loader"

# Carica i dati da HDFS a Redis
def load_to_redis(hdfs_path):
    cmd = f"docker exec {CONTAINER_NAME} python /app/load.py --directory {hdfs_path}"
    try:
        result = subprocess.run(
            cmd,
            shell=True,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        print(result.stdout)
    except subprocess.CalledProcessError as e:
        print("Errore durante l'esecuzione dello script")
        print(e.stderr)


# Esporta i dati da Redis nella cartella specificata
def export_from_redis(output_dir):
    cmd = f"docker exec {CONTAINER_NAME} python /app/export.py --directory {output_dir}"
    try:
        result = subprocess.run(
            cmd,
            shell=True,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        print(result.stdout)
    except subprocess.CalledProcessError as e:
        print("Errore durante l'esecuzione dello script")
        print(e.stderr)