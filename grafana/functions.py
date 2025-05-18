import os
import subprocess
from pathlib import Path

def export_hdfs_csv_to_grafana(script_path: str, hdfs_uri: str):

    query_name = Path(script_path).stem  # estrae ad esempio "query3" da "/app/query3.py"

    hdfs_path = hdfs_uri.replace("hdfs://namenode:9000", "")
    local_dir = "./grafana/csv"
    local_path = f"{local_dir}/{query_name}.csv"
    hdfs_tmp_path = f"/tmp/{query_name}.csv"

    os.makedirs(local_dir, exist_ok=True)

    print(f"📥 Eseguo il merge HDFS: {hdfs_path} → {hdfs_tmp_path} (in container)")
    merge_cmd = f"docker exec namenode hdfs dfs -getmerge {hdfs_path} {hdfs_tmp_path}"
    result = subprocess.run(merge_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding="utf-8", errors="replace")

    if result.returncode != 0:
        print(f" Errore durante il getmerge da HDFS:\n{result.stderr}")
        return

    print(f"📤 Copio il CSV da namenode → {local_path}")
    cp_cmd = f"docker cp namenode:{hdfs_tmp_path} {local_path}"
    cp_result = subprocess.run(cp_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding="utf-8", errors="replace")

    if cp_result.returncode != 0:
        print(f" Errore nel docker cp:\n{cp_result.stderr}")
    else:
        print(f" CSV esportato correttamente in {local_path} (Grafana ready)")
