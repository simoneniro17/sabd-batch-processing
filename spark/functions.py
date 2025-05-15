import subprocess

SPARK_HOST = "spark-master"
SPARK_SUBMIT_PATH = "/opt/spark/bin/spark-submit"

def execute_spark_query(script, input_paths, output_path, zone, runs):
    if script.endswith("query1.py"):
        if not isinstance(input_paths, tuple) or len(input_paths) != 2:
            raise ValueError("Per la query 1, input_paths deve essere una tupla con due percorsi.")

        it_path, se_path = input_paths
        spark_submit_cmd = f"docker exec {SPARK_HOST} {SPARK_SUBMIT_PATH} --master spark://spark-master:7077 \
                                {script} --input_it {it_path} --input_se {se_path} --output {output_path} --runs {runs}"
    else:
        if zone == "":
            spark_submit_cmd = f"docker exec {SPARK_HOST} {SPARK_SUBMIT_PATH} --master spark://spark-master:7077 \
                                    {script} --input {input_paths} --output {output_path} --runs {runs}"
        else:
            spark_submit_cmd = f"docker exec {SPARK_HOST} {SPARK_SUBMIT_PATH} --master spark://spark-master:7077 \
                                    {script} --input {input_paths} --output {output_path} --zone {zone} --runs {runs}"
    
    subprocess.run(spark_submit_cmd, shell=True, check=True)
