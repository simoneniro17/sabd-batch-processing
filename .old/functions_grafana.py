import os
import subprocess
from pathlib import Path

def export_query_result_to_grafana(query_path, output_dir):
    """
    Esporta i file CSV risultanti da una query Spark da HDFS a una cartella locale per l'utilizzo con Grafana.
    """

    # Definizione dei percorsi
    query_name = Path(query_path).stem  
    hdfs_tmp_path = f"/tmp/{query_name}.csv"
    local_dir = "./grafana/csv"
    local_path = f"{local_dir}/{query_name}.csv"

    os.makedirs(local_dir, exist_ok=True)
   
    # Estrazione del CSV da HDFS
    merge_cmd = f"docker exec namenode hdfs dfs -getmerge {output_dir} {hdfs_tmp_path}"
    result = subprocess.run(
        merge_cmd,
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        encoding="utf-8"
    )

    if result.returncode != 0:
        print(f" Errore durante l'estrazione da HDFS:\n{result.stderr}")
        return

    # Copia dal container alla macchina host
    cp_cmd = f"docker cp namenode:{hdfs_tmp_path} {local_path}"
    cp_result = subprocess.run(
        cp_cmd,
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        encoding="utf-8"
    )

    if cp_result.returncode != 0:
        print(f"Errore durante la copia dal container:\n{cp_result.stderr}")
    else:
        print(f"CSV esportato correttamente in {local_path}")