import subprocess

SPARK_HOST = "spark-master"
SPARK_SUBMIT_PATH = "/opt/spark/bin/spark-submit"


# def find_spark_submit():
#     """Trova il percorso di spark-submit all'interno del container"""
#     print("🔍 Cerco spark-submit nel container spark-master...")
#     cmd = f"docker exec {SPARK_HOST} find / -name spark-submit 2>/dev/null"
#     result = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
#     path = result.stdout.decode().strip()
#     if path:
#         print(f"✅ Trovato spark-submit in: {path}")
#     else:
#         print("❌ spark-submit non trovato!")
#         raise FileNotFoundError("spark-submit non trovato nel container.")
#     return path

def execute_spark_query(script, input_path, output_path, zone):
    spark_submit_cmd = f"docker exec {SPARK_HOST} {SPARK_SUBMIT_PATH} --master spark://spark-master:7077 {script} --input {input_path} --output {output_path} --zone {zone}"
    subprocess.run(spark_submit_cmd, shell=True, check=True)





