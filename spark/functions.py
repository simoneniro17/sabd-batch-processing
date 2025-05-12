import subprocess

SPARK_HOST = "spark-master"


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

def run_spark_job(spark_submit_path, query):
    """Esegue il job Spark usando spark-submit"""
    spark_submit_cmd = f"docker exec {SPARK_HOST} {spark_submit_path} {query} --master spark://spark-master:7077"
    subprocess.run(spark_submit_cmd)

# TODO: aggiungere funzione che sposta l'output all'interno di hdfs (utilizza quello che sono già in hdfs/functions.py come base)

def execute_spark_query(query):
    """Esegue una query Spark"""
    spark_submit_path = find_spark_submit()
    run_spark_job(spark_submit_path, query)