import subprocess

CONTAINER_NAME = "redis-loader"

def load_to_redis(hdfs_path):
    cmd = f"docker exec {CONTAINER_NAME} python /app/load.py --directory {hdfs_path}"
    print(f"Eseguendo il comando: {cmd}")
    try:
        result = subprocess.run(
            cmd,
            shell=True,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        print("Salvataggio completato con successo.")
        print(result.stdout)
    except subprocess.CalledProcessError as e:
        print("Errore durante l'esecuzione dello script")
        print(e.stderr)

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
        print("Esportazione completata con successo.")
        print(result.stdout)
    except subprocess.CalledProcessError as e:
        print("Errore durante l'esecuzione dello script")
        print(e.stderr)