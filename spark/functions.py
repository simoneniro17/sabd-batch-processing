import subprocess

SPARK_HOST = "spark-master"
SPARK_SUBMIT_PATH = "/opt/spark/bin/spark-submit"
PY_FILES = "--py-files /app/evaluation.py"

def execute_spark_query(script, input_paths, output_path, runs):

    if script.endswith("query1.py") or script.endswith("query3.py"):
        if not isinstance(input_paths, tuple) or len(input_paths) != 2:
            raise ValueError("Per questa query, il parametro input_paths deve essere una tupla con i percorsi di IT e SE.")

        it_path, se_path = input_paths
        spark_submit_cmd = f"docker exec {SPARK_HOST} {SPARK_SUBMIT_PATH} --master spark://spark-master:7077 \
            {PY_FILES} {script} --input_it {it_path} --input_se {se_path} --output {output_path} --runs {runs}"
    else:
        spark_submit_cmd = f"docker exec {SPARK_HOST} {SPARK_SUBMIT_PATH} --master spark://spark-master:7077 \
            {PY_FILES} {script} --input {input_paths} --output {output_path} --runs {runs}"

    subprocess.run(spark_submit_cmd, shell=True, check=True)
