import os
import redis
import argparse

# TODO: rimuovi i debug

def export_from_redis(output_dir):
    r = redis.Redis(host="redis", port=6379, decode_responses=True)
    keys = r.keys("hdfs_data:*")
    print(f"[DEBUG] Trovate {len(keys)} chiavi")

    os.makedirs(output_dir, exist_ok=True)

    for key in keys:
        if key.startswith("hdfs_data:"):
            key_no_prefix = key[len("hdfs_data:"):]  # togli prefisso
        else:
            key_no_prefix = key
        filename = os.path.join(output_dir, key_no_prefix)
        data = r.get(key)
        with open(filename, "w", encoding="utf-8") as f:
            f.write(data)
        print(f"[DEBUG] Salvato: {filename}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Esporta e salva i dati da Redis.")
    parser.add_argument("--directory", required=True, help="Percorso della cartella di esportare (es. /app/results)")

    args = parser.parse_args()

    print(f"[DEBUG] Avvio script con output_path = {args.directory}")
    export_from_redis(args.directory)