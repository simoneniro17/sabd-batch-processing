import subprocess

SPARK_HOST = "spark-master"
SPARK_SUBMIT_PATH = "/opt/spark/bin/spark-submit"

def execute_spark_query(script, input_path, output_path, zone, runs):
    if zone == "":
        spark_submit_cmd = f"docker exec {SPARK_HOST} {SPARK_SUBMIT_PATH} --master spark://spark-master:7077 \
                                {script} --input {input_path} --output {output_path}"   # TODO: aggiungere runs
    else:
        spark_submit_cmd = f"docker exec {SPARK_HOST} {SPARK_SUBMIT_PATH} --master spark://spark-master:7077 \
                                {script} --input {input_path} --output {output_path} --zone {zone} --runs {runs}"
    
    subprocess.run(spark_submit_cmd, shell=True, check=True)
