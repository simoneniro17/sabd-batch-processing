import os
import redis
import argparse
import csv
from io import StringIO

def export_from_redis(output_dir):
    r = redis.Redis(host="redis", port=6379, decode_responses=True)
    keys = r.keys("hdfs_data:*")

    evaluation_rows = []
    header_seen = False
    expected_header = ['query-id', 'query-type', 'num-runs', 'time-avg (s)', 'time-min (s)', 'time-max (s)', 'std-dev (s)']

    os.makedirs(output_dir, exist_ok=True)

    for key in keys:
        key_no_prefix = key[len("hdfs_data:"):] if key.startswith("hdfs_data:") else key
        data = r.get(key)

        if data is None:
            continue

        # Gestione dei file evaluation_*.csv
        if key_no_prefix.startswith("evaluation_") and key_no_prefix.endswith(".csv"):
            f = StringIO(data)
            reader = csv.reader(f)
            try:
                header = next(reader)
                if header == expected_header:
                    if not header_seen:
                        evaluation_rows.append(header)
                        header_seen = True
                    evaluation_rows.extend(row for row in reader if row)
            except StopIteration:
                continue  # Salta i file vuoti
        else:
            filename = os.path.join(output_dir, key_no_prefix)
            with open(filename, "w", encoding="utf-8") as f:
                f.write(data)

    # Scriviamo il file evaluation aggregato
    if evaluation_rows:
        evaluation_path = os.path.join(output_dir, "evaluation.csv")
        with open(evaluation_path, "w", encoding="utf-8", newline='') as f:
            writer = csv.writer(f)
            writer.writerows(evaluation_rows)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Esporta e salva i dati da Redis.")
    parser.add_argument("--directory", required=True, help="Percorso della cartella in cui si vogliono esportare i dati (e.g. ./results).")
    args = parser.parse_args()

    export_from_redis(args.directory)
