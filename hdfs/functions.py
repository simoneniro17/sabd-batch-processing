#TODO: Quesdto file non serve più, teniamolo non si sa mai torna utile ma poi si può cancellare 


import subprocess

HDFS_HOST = "namenode"
HDFS_DIR = "/data"

def copy_file_to_namenode(path):
    """Copia il file locale nel container HDFS"""
    subprocess.run(f"docker cp {path} {HDFS_HOST}:/tmp/input1.txt")

def create_hdfs_dir():
    """Crea la directory su HDFS"""
    subprocess.run(f"docker exec {HDFS_HOST} hdfs dfs -mkdir -p {HDFS_DIR}")

def file_exists_on_hdfs(path):
    """Verifica se il file esiste su HDFS"""
    cmd = f"docker exec {HDFS_HOST} hdfs dfs -test -e {path}"
    result = subprocess.run(cmd, shell=True)
    return result.returncode == 0

def put_file_on_hdfs():
    """Carica il file su HDFS"""
    if file_exists_on_hdfs():
        print("✅ Il file è già presente su HDFS. Salto il put.")
    else:
        subprocess.run(f"docker exec {HDFS_HOST} hdfs dfs -put /tmp/input1.txt {HDFS_FILE}")
