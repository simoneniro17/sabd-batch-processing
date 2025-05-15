from nifi.functions import feed_nifi_urls
from spark.functions import execute_spark_query
from redis.functions import load_to_redis

import subprocess
import argparse

#-------NIFI-------

# # carica il tamplate e avvialo
cmd = f"docker exec nifi ../scripts/import-template.sh"
result = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

print("STDOUT:", result.stdout)
# print("STDERR:", result.stderr)
print("Return code:", result.returncode)

# # # # manda i dati da processare a nifi
#feed_nifi_urls()

#--------SPARK-------

script_q1 = "/app/query1.py"
script_q2 = "/app/query2.py"
script_q3 = "/app/query3.py"

# TODO: OPZIONALE: per rendere meno hardcoded il tutto possiamo fare che è direttamente da spark/functions che si monta il path di hdfs://namenode:9000/ leggendolo dinamicamnte dal container

input_files_q1 = (
    "hdfs://namenode:9000/data/IT_hourly.parquet",
    "hdfs://namenode:9000/data/SE_hourly.parquet"
)
output_dir_q1 = "hdfs://namenode:9000/results/query1"

input_files_q2 = (
    "hdfs://namenode:9000/data/IT_2021_hourly.parquet"
    ",hdfs://namenode:9000/data/IT_2022_hourly.parquet"
    ",hdfs://namenode:9000/data/IT_2023_hourly.parquet"
    ",hdfs://namenode:9000/data/IT_2024_hourly.parquet"
)
output_dir_q2 = "hdfs://namenode:9000/results/query2"

input_files_q3 = (
    "hdfs://namenode:9000/data/IT_2021_hourly.parquet"
    ",hdfs://namenode:9000/data/IT_2022_hourly.parquet"
    ",hdfs://namenode:9000/data/IT_2023_hourly.parquet"
    ",hdfs://namenode:9000/data/IT_2024_hourly.parquet"
)
output_dir_q3 = "hdfs://namenode:9000/results/query3"

execute_spark_query(script_q1, input_files_q1, output_dir_q1, "", runs = 1)
#execute_spark_query(script_q2, input_files_q2, output_dir_q2, "", runs)
#execute_spark_query(script_q3, input_files_q3, output_dir_q3, zone_id, runs)

#--------REDIS-------
hdfs_results_path = "/results/"
load_to_redis(hdfs_results_path)

# se vuoi controllare se effettivamnte i file sono stati salvati segui: 
#docker exec -it redis redis-cli
# e poi dalla shell di redis:
# keys hdfs_data:*
# se vuoi cancellare tutte le chiavi:
# FLUSHDB