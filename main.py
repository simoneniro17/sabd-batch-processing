from nifi.functions import feed_nifi_urls
from spark.functions import execute_spark_query
from redis.functions import load_to_redis
from grafana.functions import export_hdfs_csv_to_grafana
import subprocess
import argparse

#-------NIFI-------

# # carica il tamplate e avvialo
# cmd = f"docker exec nifi ../scripts/import-template.sh"
# result = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

# print("STDOUT:", result.stdout)
# print("STDERR:", result.stderr)
# print("Return code:", result.returncode)

# # manda i dati da processare a nifi
# feed_nifi_urls()

#--------SPARK-------

script_q1 = "/app/query1.py"
script_q2 = "/app/query2.py"
script_q3 = "/app/query3.py"
script_q4 = "/app/query4.py"

script_q1_sql = "/app/query1_sql.py"
script_q2_sql = "/app/query2_sql.py"
script_q3_sql = "/app/query3_sql.py"

# TODO: OPZIONALE: per rendere meno hardcoded il tutto possiamo fare che è direttamente da spark/functions che si monta il path di hdfs://namenode:9000/ leggendolo dinamicamnte dal container

IT_HOURLY = "hdfs://namenode:9000/data/IT_hourly.parquet"
SE_HOURLY = "hdfs://namenode:9000/data/SE_hourly.parquet"
YEARLY = "hdfs://namenode:9000/data/2024_yearly.csv"

output_dir_q1 = "hdfs://namenode:9000/results/query1"
output_dir_q2 = "hdfs://namenode:9000/results/query2"
output_dir_q3 = "hdfs://namenode:9000/results/query3"
output_dir_q4 = "hdfs://namenode:9000/results/query4"

output_dir_q1_sql = "hdfs://namenode:9000/results/query1-sql"
output_dir_q2_sql = "hdfs://namenode:9000/results/query2-sql"
output_dir_q3_sql = "hdfs://namenode:9000/results/query3-sql"

execute_spark_query(script_q1, (IT_HOURLY, SE_HOURLY), output_dir_q1, runs = 1)
#execute_spark_query(script_q2, IT_HOURLY, output_dir_q2, runs = 1)
#execute_spark_query(script_q3, (IT_HOURLY, SE_HOURLY), output_dir_q3, runs = 1)
#execute_spark_query(script_q4, YEARLY, output_dir_q4, runs = 1)
#execute_spark_query(script_q1_sql, (IT_HOURLY, SE_HOURLY), output_dir_q1_sql, runs = 1)
#execute_spark_query(script_q2_sql, IT_HOURLY, output_dir_q2_sql, runs = 1)
#execute_spark_query(script_q3_sql, (IT_HOURLY, SE_HOURLY), output_dir_q3_sql, runs = 1)

#--------REDIS-------
# hdfs_results_path = "/results/"
# load_to_redis(hdfs_results_path)

#--------GRAFANA-------
export_hdfs_csv_to_grafana(script_q1, output_dir_q1)
#export_hdfs_csv_to_grafana(script_q2, output_dir_q2)
#export_hdfs_csv_to_grafana(script_q3, output_dir_q3)
#export_hdfs_csv_to_grafana(script_q4, output_dir_q4)


# se vuoi controllare se effettivamnte i file sono stati salvati segui: 
#docker exec -it redis redis-cli
# e poi dalla shell di redis:
# keys hdfs_data:*
# se vuoi cancellare tutte le chiavi:
# FLUSHDB