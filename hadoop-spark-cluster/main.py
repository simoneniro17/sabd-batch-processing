import subprocess
import os

HDFS_DIR = "/input"
HDFS_FILE = f"{HDFS_DIR}/input1.txt"
LOCAL_FILE = "input1.txt"
HDFS_HOST = "namenode"
SPARK_HOST = "spark-master"
SPARK_SCRIPT = "/app/spark_job.py"  # Path corretto nel container Spark

def run(cmd, check=True):
    """Esegue un comando di sistema e gestisce gli errori"""
    print(f"🔧 Eseguo: {cmd}")
    result = subprocess.run(cmd, shell=True, check=check, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    print(result.stdout.decode())
    return result

def check_container_status(container_name):
    """Verifica se un container è in esecuzione"""
    cmd = f"docker ps --filter 'name={container_name}' --filter 'status=running' -q"
    result = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    return result.stdout.decode().strip() != ''

def find_spark_submit():
    """Trova il percorso di spark-submit all'interno del container"""
    print("🔍 Cerco spark-submit nel container spark-master...")
    cmd = f"docker exec {SPARK_HOST} find / -name spark-submit 2>/dev/null"
    result = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    path = result.stdout.decode().strip()
    if path:
        print(f"✅ Trovato spark-submit in: {path}")
    else:
        print("❌ spark-submit non trovato!")
        raise FileNotFoundError("spark-submit non trovato nel container.")
    return path

def copy_file_to_namenode():
    """Copia il file locale nel container HDFS"""
    run(f"docker cp {LOCAL_FILE} {HDFS_HOST}:/tmp/input1.txt")

def create_hdfs_dir():
    """Crea la directory su HDFS"""
    run(f"docker exec {HDFS_HOST} hdfs dfs -mkdir -p {HDFS_DIR}")

def file_exists_on_hdfs():
    """Verifica se il file esiste su HDFS"""
    cmd = f"docker exec {HDFS_HOST} hdfs dfs -test -e {HDFS_FILE}"
    result = subprocess.run(cmd, shell=True)
    return result.returncode == 0

def put_file_on_hdfs():
    """Carica il file su HDFS"""
    if file_exists_on_hdfs():
        print("✅ Il file è già presente su HDFS. Salto il put.")
    else:
        run(f"docker exec {HDFS_HOST} hdfs dfs -put /tmp/input1.txt {HDFS_FILE}")

def run_spark_job(spark_submit_path):
    """Esegue il job Spark usando spark-submit"""
    spark_submit_cmd = f"docker exec {SPARK_HOST} {spark_submit_path} {SPARK_SCRIPT} --master spark://spark-master:7077"
    run(spark_submit_cmd)

def main():
    """Funzione principale che coordina il flusso del programma"""
    if not os.path.exists(LOCAL_FILE):
        print(f"❌ Il file {LOCAL_FILE} non esiste nella directory locale.")
        return

    # Verifica se il container Spark è in esecuzione
    if not check_container_status(SPARK_HOST):
        print(f"❌ Il container {SPARK_HOST} non è in esecuzione. Avvia prima il container.")
        return

    copy_file_to_namenode()
    create_hdfs_dir()
    put_file_on_hdfs()

    try:
        spark_submit_path = find_spark_submit()
        run_spark_job(spark_submit_path)
    except FileNotFoundError as e:
        print(e)

if __name__ == "__main__":
    main()
