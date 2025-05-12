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

def run_spark_job(spark_submit_path, script, input_path, output_path):
    """Esegue il job Spark usando spark-submit"""
    # Costruzione del comando per eseguire lo script con i parametri
    spark_submit_cmd = f"docker exec {SPARK_HOST} {spark_submit_path} --master spark://spark-master:7077 {script} --input {input_path} --output {output_path}"
    subprocess.run(spark_submit_cmd, shell=True, check=True)


# TODO: aggiungere funzione che sposta l'output all'interno di hdfs (utilizza quello che sono già in hdfs/functions.py come base)

def execute_spark_query(script, input_path, output_path):
    """Esegue una query Spark passando i parametri di input, output e script"""
    spark_submit_path = find_spark_submit()  # Trova il percorso di spark-submit
    run_spark_job(spark_submit_path, script, input_path, output_path)



