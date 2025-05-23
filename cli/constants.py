# Costanti e configurazioni utili per la CLI

CONFIG = {
    "input_paths": {
        "it_hourly": "/data/IT_hourly.parquet",
        "se_hourly": "/data/SE_hourly.parquet",
        "yearly": "/data/2024_yearly.csv"
    },
    "output_paths": {
        "query1": "/results/query1",
        "query1_sql": "/results/query1-sql",
        "query2": "/results/query2",
        "query2_sql": "/results/query2-sql",
        "query3": "/results/query3",
        "query3_sql": "/results/query3-sql",
        "query4": "/results/query4"
    },
    "scripts": {
        "query1": "/app/query1.py",
        "query1_sql": "/app/query1_sql.py",
        "query2": "/app/query2.py",
        "query2_sql": "/app/query2_sql.py",
        "query3": "/app/query3.py",
        "query3_sql": "/app/query3_sql.py",
        "query4": "/app/query4.py"
    },
    "redis": {
        "hdfs_path": "/results/",
        "local_path": "/app/results"
    },
    "default_runs": 1,
    "query_descriptions": {
        "1": "Media, massimo e minimo per IT e SE per ciascun anno dal 2021 al 2024 con dati aggregati su base annua",
        "2": "Statistiche sulla coppia (anno, mese) e classifiche delle prime 5 coppie per IT",
        "3": "Statistiche con dati aggregati su un periodo di 24 ore per IT e SE",
        "4": "Clustering di paesi con dati su base annua e per l'anno 2024 (solo DataFrame)"
    },
    "mode_labels": {
        "no_sql": "DataFrame",
        "sql": "SQL"
    },
    "host_results_dir": "./results"
}


# I codici seguono lo standard ANSI escape sequences, dove \033[ è il carattere di escape che indica l'inizio di una sequenza ANSI.
# È importante usare ENDC alla fine per ripristinare lo stile normale, altrimenti gli effetti continueranno a essere applicati.
class Colors:
    ENDC = '\033[0m'            # Testo per terminare tutti gli effetti di formattazione
    BOLD = '\033[1m'            # Testo in grassetto
    UNDERLINE = '\033[4m'       # Testo sottolineato
    
    RED = '\033[91m'            # Testo rosso
    GREEN = '\033[92m'          # Testo verde
    YELLOW = '\033[93m'         # Testo giall
    BLUE = '\033[94m'           # Testo blu
    HEADER = '\033[95m'         # Testo viola/magenta per l'intestazione
    CYAN = '\033[96m'           # Testo ciano
