# carica i dati da nifi (comunque controlli se sono già presenti) a hdfs
# spark li prende e fa la query
# carica i risultati su hdfs
# sposta i dati su redis

from nifi.functions import feed_nifi_urls
from spark.functions import execute_spark_query
from redis.functions import load_to_redis


import subprocess
import argparse

#-------NIFI-------

# TODO: fase di controllo che veda se i dati sono già presenti e quindi non riesegua (utilizza quello che sono già in hdfs/functions.py come base)

# TODO: dobbiamo vedere import template per fargli almeno caricare il template
# carica il tamplate e avvialo
# result = subprocess.run(["/bin/bash", "nifi/nifi_start.sh"], capture_output=True, text=True)

# print("STDOUT:", result.stdout)
# print("STDERR:", result.stderr)
# print("Return code:", result.returncode)

# manda i dati da processare a nifi
feed_nifi_urls("LONG")

#--------SPARK-------
app = "/app/spark_job.py"
input_file = "hdfs://namenode:9000/data/AE_2024_hourly.csv"
output_dir = "hdfs://namenode:9000/results/line_count"
# TODO: dobbiamo modificare per far si che possa prendere più argomenti di input/output
execute_spark_query(app, input_file, output_dir)

#--------REDIS-------
#load_to_redis()