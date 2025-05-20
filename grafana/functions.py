import os
import subprocess
from pathlib import Path

def export_hdfs_csv_to_grafana(script_path: str, output_dir: str):
    query_name = Path(script_path).stem  # es. "query3" da "/app/query3.py"
    
    # Costruisce internamente il path completo HDFS
    hdfs_base = "hdfs://namenode:9000"
    hdfs_uri = f"{hdfs_base}{output_dir}"
    hdfs_path = output_dir  # es. "/results/query3"
    hdfs_tmp_path = f"/tmp/{query_name}.csv"
    local_dir = "./grafana/csv"
    local_path = f"{local_dir}/{query_name}.csv"

    os.makedirs(local_dir, exist_ok=True)

   
    merge_cmd = f"docker exec namenode hdfs dfs -getmerge {hdfs_path} {hdfs_tmp_path}"
    result = subprocess.run(merge_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding="utf-8")

    if result.returncode != 0:
        print(f" Errore durante il getmerge da HDFS:\n{result.stderr}")
        return
    cp_cmd = f"docker cp namenode:{hdfs_tmp_path} {local_path}"
    cp_result = subprocess.run(cp_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding="utf-8")

    if cp_result.returncode != 0:
        print(f" Errore nel docker cp:\n{cp_result.stderr}")
    else:
        print(f" CSV esportato correttamente in {local_path} (Grafana ready)")