import os
import redis
import csv
import sys
import traceback

def export_from_redis(output_dir="/app/results"):
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
    output_path = sys.argv[1] if len(sys.argv) > 1 else "/app/results"
    print(f"[DEBUG] Avvio script con output_path = {output_path}")
    export_from_redis(output_path)