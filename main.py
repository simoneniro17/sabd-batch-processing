from nifi.functions import feed_nifi_urls
from spark.functions import execute_spark_query
from redis.functions import load_to_redis
from grafana.functions import export_hdfs_csv_to_grafana
import subprocess
import argparse

# #-------NIFI-------

# # # carica il tamplate e avvialo
# # cmd = f"docker exec nifi ../scripts/import-template.sh"
# # result = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

# # print("STDOUT:", result.stdout)
# # print("STDERR:", result.stderr)
# # print("Return code:", result.returncode)

# # # manda i dati da processare a nifi
# # feed_nifi_urls()

# #--------SPARK-------

# script_q1 = "/app/query1.py"
# script_q2 = "/app/query2.py"
# script_q3 = "/app/query3.py"
# script_q4 = "/app/query4.py"

# script_q1_sql = "/app/query1_sql.py"
# script_q2_sql = "/app/query2_sql.py"
# script_q3_sql = "/app/query3_sql.py"

# IT_HOURLY = "/data/IT_hourly.parquet"
# SE_HOURLY = "/data/SE_hourly.parquet"
# YEARLY = "/data/2024_yearly.csv"

# output_dir_q1 = "/results/query1"
# output_dir_q2 = "/results/query2"
# output_dir_q3 = "/results/query3"
# output_dir_q4 = "/results/query4"

# output_dir_q1_sql = "/results/query1-sql"
# output_dir_q2_sql = "/results/query2-sql"
# output_dir_q3_sql = "/results/query3-sql"

# #execute_spark_query(script_q1, (IT_HOURLY, SE_HOURLY), output_dir_q1, runs = 1)
# #execute_spark_query(script_q2, IT_HOURLY, output_dir_q2, runs = 1)
# #execute_spark_query(script_q3, (IT_HOURLY, SE_HOURLY), output_dir_q3, runs = 1)
# #execute_spark_query(script_q4, YEARLY, output_dir_q4, runs = 1)
# #execute_spark_query(script_q1_sql, (IT_HOURLY, SE_HOURLY), output_dir_q1_sql, runs = 1)
# #execute_spark_query(script_q2_sql, IT_HOURLY, output_dir_q2_sql, runs = 1)
# #execute_spark_query(script_q3_sql, (IT_HOURLY, SE_HOURLY), output_dir_q3_sql, runs = 1)

# #--------REDIS-------
# hdfs_results_path = "/results/"
# load_to_redis(hdfs_results_path)

# #--------GRAFANA-------
# #export_hdfs_csv_to_grafana(script_q1, output_dir_q1)
# #export_hdfs_csv_to_grafana(script_q2, output_dir_q2)
# #export_hdfs_csv_to_grafana(script_q3, output_dir_q3)
# #export_hdfs_csv_to_grafana(script_q4, output_dir_q4)


# # se vuoi controllare se effettivamnte i file sono stati salvati segui: 
# #docker exec -it redis redis-cli
# # e poi dalla shell di redis:
# # keys hdfs_data:*
# # se vuoi cancellare tutte le chiavi:
# # FLUSHDB


import subprocess
from nifi.functions import feed_nifi_urls
from spark.functions import execute_spark_query
from redis.functions import load_to_redis
from grafana.functions import export_hdfs_csv_to_grafana


def import_nifi_template():
    print("Importazione template NiFi...")
    cmd = f"docker exec nifi ../scripts/import-template.sh"
    result = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    print("STDOUT:", result.stdout.decode())
    print("STDERR:", result.stderr.decode())
    print("Return code:", result.returncode)

def start_nifi():
    import_nifi_template()
    print("Feeding dati a NiFi...")
    feed_nifi_urls()
    
def run_spark_menu():
    scripts = {
        "1": {
            "no_sql": ("/app/query1.py", ("/data/IT_hourly.parquet", "/data/SE_hourly.parquet"), "/results/query1"),
            "sql": ("/app/query1_sql.py", ("/data/IT_hourly.parquet", "/data/SE_hourly.parquet"), "/results/query1-sql"),
        },
        "2": {
            "no_sql": ("/app/query2.py", "/data/IT_hourly.parquet", "/results/query2"),
            "sql": ("/app/query2_sql.py", "/data/IT_hourly.parquet", "/results/query2-sql"),
        },
        "3": {
            "no_sql": ("/app/query3.py", ("/data/IT_hourly.parquet", "/data/SE_hourly.parquet"), "/results/query3"),
            "sql": ("/app/query3_sql.py", ("/data/IT_hourly.parquet", "/data/SE_hourly.parquet"), "/results/query3-sql"),
        },
        "4": {
            "no_sql": ("/app/query4.py", "/data/2024_yearly.csv", "/results/query4"),
            "sql": None
        }
    }

    while True:
        query = input("Quale query vuoi lanciare? (1, 2, 3, 4) o 'back' per tornare: ").strip()
        if query == "back":
            break
        if query not in scripts:
            print("Scelta non valida.")
            continue

        mode = input("In che modalità? (sql / no_sql): ").strip()
        if mode not in ["sql", "no_sql"]:
            print("Modalità non valida.")
            continue

        script_info = scripts[query].get(mode)
        if script_info is None:
            print("Modalità non disponibile per questa query.")
            continue

        script, input_data, output_dir = script_info
        print(f"Eseguo Query {query} in modalità {mode}...")
        execute_spark_query(script, input_data, output_dir, runs=1)
        print("Query completata.\n")

def run_redis_menu():
    while True:
        choice = input("Redis - (1) Caricare su Redis | (2) Scaricare da Redis | (3) Back: ").strip()
        if choice == "1":
            print("Caricamento su Redis...")
            load_to_redis("/results/")
        elif choice == "2":
            print("Download da Redis...")
            print("TODO: implementare download da Redis")
        elif choice == "3":
            break
        else:
            print("Scelta non valida.")

def main_menu():
    print("=== AVVIO PROGETTO ===")
    start_nifi()

    while True:
        print("\nCosa vuoi fare?")
        print("1. Spark")
        print("2. Redis")
        print("3. Esci")

        choice = input("Scelta: ").strip()

        if choice == "1":
            run_spark_menu()
        elif choice == "2":
            run_redis_menu()
        elif choice == "3":
            print("Uscita.")
            break
        else:
            print("Scelta non valida.")

if __name__ == "__main__":
    main_menu()
