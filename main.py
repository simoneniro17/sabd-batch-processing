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

# carica il tamplate e avvialo
# cmd = f"docker exec nifi ../scripts/import-template.sh"
# result = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

# print("STDOUT:", result.stdout)
# print("STDERR:", result.stderr)
# print("Return code:", result.returncode)

# # manda i dati da processare a nifi
# feed_nifi_urls()

#--------SPARK-------
app = "/app/query1.py"
input_files = (
    "hdfs://namenode:9000/data/IT_2021_hourly.csv,"
    "hdfs://namenode:9000/data/IT_2022_hourly.csv"
)
output_dir = "hdfs://namenode:9000/results/query1-IT"
zone_id = "IT"

print(input_files)

execute_spark_query(app, input_files, output_dir, zone_id)


#--------REDIS-------
#load_to_redis()