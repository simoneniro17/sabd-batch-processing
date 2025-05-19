import subprocess

CONTAINER_NAME = "redis-loader"

def load_to_redis(hdfs_path):
    cmd = f"docker exec {CONTAINER_NAME} python /app/load_to_redis.py {hdfs_path}"
    print(f"🚀 Eseguo il comando: {cmd}")
    try:
        result = subprocess.run(
            cmd,
            shell=True,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        print("✅ Script completato con successo:")
        print(result.stdout)
    except subprocess.CalledProcessError as e:
        print("❌ Errore durante l'esecuzione dello script:")
        print(e.stderr)

def export_from_redis(output_dir="/app/results"):
    cmd = f"docker exec {CONTAINER_NAME} python /app/export_from_redis.py {output_dir}"
    print(f"🚀 Eseguo il comando: {cmd}")
    try:
        result = subprocess.run(
            cmd,
            shell=True,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        print("✅ Script completato con successo:")
        print(result.stdout)
    except subprocess.CalledProcessError as e:
        print("❌ Errore durante l'esecuzione dello script:")
        print(e.stderr)