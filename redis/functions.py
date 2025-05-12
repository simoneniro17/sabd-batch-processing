import subprocess

CONTAINER_NAME = "redis-loader"

# TODO: passare l'argomento dello script, l'idea è che loader/load_to_redis.py prenda in input il nome (o i nomi) dei file da caricare su redis, bisogna quindi modificare anche quello

def load_to_redis():
    cmd = f"docker exec {CONTAINER_NAME} python /app/load_to_redis.py"
    print(f"🚀 Eseguo il comando: {cmd}")
    try:
        # Esegui il comando nel container
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