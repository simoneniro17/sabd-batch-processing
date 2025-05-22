from nifi.functions import feed_nifi_urls
from spark.functions import execute_spark_query
from redis.functions import load_to_redis, export_from_redis
from grafana.functions import export_query_result_to_grafana
import subprocess
import sys
import argparse
from constants import CONFIG, Colors


def print_header(text: str):
    print(f"\n{Colors.HEADER}{Colors.BOLD}{'=' * 60}{Colors.ENDC}")
    print(f"{Colors.HEADER}{Colors.BOLD}{text.center(60)}{Colors.ENDC}")
    print(f"{Colors.HEADER}{Colors.BOLD}{'=' * 60}{Colors.ENDC}\n")

def print_info(text: str):
    """Stampa un messaggio informativo."""
    print(f"{Colors.BLUE}ℹ {text}{Colors.ENDC}")

def print_success(text: str):
    """Stampa un messaggio di successo."""
    print(f"{Colors.GREEN}✓ {text}{Colors.ENDC}")

def print_warning(text: str):
    """Stampa un avviso."""
    print(f"{Colors.YELLOW}⚠ {text}{Colors.ENDC}")

def print_error(text: str):
    """Stampa un messaggio di errore."""
    print(f"{Colors.RED}✗ {text}{Colors.ENDC}")

def print_query_info(query_num: str, mode: str):
    """Stampa le informazioni sulla query selezionata."""
    mode_label = CONFIG["mode_labels"][mode]
    query_desc = CONFIG["query_descriptions"][query_num]
    
    print(f"\n{Colors.CYAN}{Colors.BOLD}Query {query_num}: {query_desc}{Colors.ENDC}")
    print(f"{Colors.CYAN}Modalità: {mode_label}{Colors.ENDC}")

######################################################################

def import_nifi_template() -> bool:
    """Importa il template NiFi e restituisce True se l'operazione ha successo."""
    print_info("Importazione template NiFi in corso...")
    
    cmd = "docker exec nifi ../scripts/import-template.sh"
    try:
        result = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        
        if result.returncode == 0:
            print_success("Template NiFi importato con successo.")
            return True
        else:
            print_error(f"Errore durante l'importazione del template NiFi (codice {result.returncode}).")
            print_info("Dettagli errore:")
            for line in result.stderr.splitlines():
                print(f"  {line}")
            return False
    except Exception as e:
        print_error(f"Eccezione durante l'importazione del template NiFi: {str(e)}")
        return False

def feed_nifi_data() -> bool:
    """Invia dati da locale a NiFi e restituisce True se l'operazione ha successo."""
    print_info("Invio dati a NiFi in corso...")
    try:
        feed_nifi_urls()
        print_success("Dati inviati a NiFi con successo.")
        return True
    except Exception as e:
        print_error(f"Errore durante l'invio dati a NiFi: {str(e)}")
        return False

def setup_nifi() -> bool:
    """Imposta NiFi importando il template e inviando dati."""
    print_header("CONFIGURAZIONE NIFI")
    
    if not import_nifi_template():
        return False
    
    if not feed_nifi_data():
        return False
    
    return True


def execute_query(query_num: str, mode: str, runs: int = CONFIG["default_runs"]) -> bool:
    """Esegue una query Spark e restituisce True se l'operazione ha successo."""
    print_header(f"ESECUZIONE QUERY {query_num}")
    print_query_info(query_num, mode)
    
    # Costruzione percorso dello script e i parametri
    script_key = f"query{query_num}" if mode == "no_sql" else f"query{query_num}_sql"
    output_path = CONFIG["output_paths"].get(script_key)
    script_path = CONFIG["scripts"].get(script_key)
    
    if not script_path:
        print_error(f"Script non trovato per Query {query_num} in modalità {CONFIG['mode_labels'][mode]}.")
        return False
    
    # Prepara gli input basati sul numero della query
    if query_num in ["1", "3"]:
        input_data = (CONFIG["input_paths"]["it_hourly"], CONFIG["input_paths"]["se_hourly"])
    elif query_num == "2":
        input_data = CONFIG["input_paths"]["it_hourly"]
    elif query_num == "4":
        input_data = CONFIG["input_paths"]["yearly"]
    else:
        print_error(f"Configurazione input non trovata per Query {query_num}.")
        return False
    
    print_info(f"Esecuzione Query {query_num} in modalità {CONFIG['mode_labels'][mode]}...")
    print_info(f"Script: {script_path}")
    print_info(f"Input: {input_data}")
    print_info(f"Output: {output_path}")
    print_info(f"Numero esecuzioni: {runs}")
    
    try:
        execute_spark_query(script_path, input_data, output_path, runs=runs)
        print_success(f"Query {query_num} completata con successo.")
        
        print_info("Esportazione risultati a Grafana in corso...")
        export_query_result_to_grafana(script_path, output_path)
        print_success("Dati esportati a Grafana.")
        
        return True
    except Exception as e:
        print_error(f"Errore durante l'esecuzione della query: {str(e)}")
        return False

def redis_upload() -> bool:
    """Carica i dati su Redis e restituisce True se l'operazione ha successo."""
    print_header("CARICAMENTO DATI SU REDIS")
    
    try:
        hdfs_path = CONFIG["redis"]["hdfs_path"]
        print_info(f"Caricamento dati da {hdfs_path} a Redis...")
        load_to_redis(hdfs_path)
        print_success("Dati caricati su Redis con successo.")
        return True
    except Exception as e:
        print_error(f"Errore durante il caricamento su Redis: {str(e)}")
        return False

def redis_download() -> bool:
    """Scarica i dati da Redis e restituisce True se l'operazione ha successo."""
    print_header("DOWNLOAD DATI DA REDIS")
    
    try:
        local_path = CONFIG["redis"]["local_path"]
        print_info(f"Download dati da Redis a {local_path}...")
        export_from_redis(local_path)
        print_success("Dati scaricati da Redis con successo.")
        print_info(f"I file sono stati salvati in: {local_path}")
        return True
    except Exception as e:
        print_error(f"Errore durante il download da Redis: {str(e)}")
        return False

def show_help() -> None:
    """Mostra informazioni di aiuto sull'applicazione."""
    print_header("INFORMAZIONI SUL PROGETTO")
    print("""
Questo strumento consente di eseguire l'intero workflow del progetto SABD:

1. Acquisizione e ingestione dati con NiFi
2. Elaborazione dati con Spark (Query 1-4)
3. Caricamento/scaricamento dati con Redis

Puoi eseguire le query in modalità DataFrame o SQL
e visualizzare i risultati su Grafana.
    """)
    
    print_header("COMANDI DISPONIBILI")
    
    commands = [
        ("1", "Esegui Query Spark", "Esegue una delle query implementate (1-4)"),
        ("2", "Gestisci Redis", "Carica o scarica dati da Redis"),
        ("3", "Configura NiFi", "Importa template e invia dati a NiFi"),
        ("4", "Help", "Mostra questa guida"),
        ("5", "Esci", "Termina l'applicazione")
    ]
    
    for cmd, name, desc in commands:
        print(f"{Colors.BOLD}{cmd}{Colors.ENDC}: {Colors.BLUE}{name}{Colors.ENDC}")
        print(f"   {desc}")
        print()
    
    print_header("ARGOMENTI DA LINEA DI COMANDO")
    print("""
python main_cli.py [opzioni]

Opzioni:
  --query 1-4        Esegue direttamente una query specifica
  --mode dataframe|sql  Modalità di esecuzione (default: dataframe)
  --runs N           Numero di esecuzioni (default: 1)
  --setup-nifi       Configura NiFi importando template e inviando dati
  --skip-nifi        Salta l'inizializzazione di NiFi
    """)

def spark_menu() -> None:
    """Menu per l'esecuzione delle query Spark."""
    while True:
        print_header("MENU QUERY SPARK")
        
        # Mostra le query disponibili
        for q_num, desc in CONFIG["query_descriptions"].items():
            print(f"{q_num}. Query {q_num} - {desc}")
        print("0. Torna al menu principale")
        
        choice = input(f"\n{Colors.BOLD}Scelta [0-4]: {Colors.ENDC}").strip()
        
        if choice == "0":
            break
        
        if choice not in ["1", "2", "3", "4"]:
            print_warning("Scelta non valida. Inserisci un numero da 0 a 4.")
            continue
        
        # Per Query 4 non c'è SQL
        modes = ["1", "2"] if choice != "4" else ["1"]
        
        while True:
            print("\nModalità di esecuzione:")
            print(f"1. {CONFIG['mode_labels']['no_sql']}")
            if choice != "4":
                print(f"2. {CONFIG['mode_labels']['sql']}")
            print("0. Torna alla selezione query")
            
            mode_choice = input(f"\n{Colors.BOLD}Scelta [0-{len(modes)}]: {Colors.ENDC}").strip()
            
            if mode_choice == "0":
                break
            
            if mode_choice not in modes:
                print_warning(f"Scelta non valida. Inserisci un numero da 0 a {len(modes)}.")
                continue
            
            mode = "no_sql" if mode_choice == "1" else "sql"
            
            runs = input(f"\n{Colors.BOLD}Numero di esecuzioni [1]: {Colors.ENDC}").strip()
            runs = int(runs) if runs.isdigit() and int(runs) > 0 else 1
            
            execute_query(choice, mode, runs)
            
            input(f"\n{Colors.BOLD}Premi Invio per continuare...{Colors.ENDC}")
            break

def redis_menu() -> None:
    """Menu per le operazioni su Redis."""
    while True:
        print_header("MENU REDIS")
        print("1. Carica dati su Redis (da HDFS)")
        print("2. Scarica dati da Redis (in locale)")
        print("0. Torna al menu principale")
        
        choice = input(f"\n{Colors.BOLD}Scelta [0-2]: {Colors.ENDC}").strip()
        
        if choice == "1":
            redis_upload()
        elif choice == "2":
            redis_download()
        elif choice == "0":
            break
        else:
            print_warning("Scelta non valida. Inserisci un numero da 0 a 2.")
            continue
        
        input(f"\n{Colors.BOLD}Premi Invio per continuare...{Colors.ENDC}")

def main_menu(nifi_configured: bool = False) -> None:
    """Menu principale dell'applicazione."""    
    while True:
        print_header("MENU PRINCIPALE")
        print("1. Esegui Query Spark")
        print("2. Gestisci Redis")
        print("3. Configura NiFi")
        print("4. Help / Informazioni")
        print("5. Esci")
        
        if not nifi_configured:
            print(f"\n{Colors.YELLOW}⚠ NiFi non ancora configurato. Usa l'opzione 3 per configurarlo.{Colors.ENDC}")
        
        choice = input(f"\n{Colors.BOLD}Scelta [1-5]: {Colors.ENDC}").strip()
        
        if choice == "1":
            spark_menu()
        elif choice == "2":
            redis_menu()
        elif choice == "3":
            nifi_configured = setup_nifi()
        elif choice == "4":
            show_help()
            input(f"\n{Colors.BOLD}Premi Invio per continuare...{Colors.ENDC}")
        elif choice == "5":
            print_info("Grazie per aver usato l'applicazione. Arrivederci!")
            break
        else:
            print_warning("Scelta non valida. Inserisci un numero da 1 a 5.")

def parse_args() -> argparse.Namespace:
    """Parsa gli argomenti della riga di comando."""
    parser = argparse.ArgumentParser(description="SABD Batch Processing CLI")
    parser.add_argument("--skip-nifi", action="store_true", help="Salta l'inizializzazione di NiFi")
    parser.add_argument("--setup-nifi", action="store_true", help="Configura NiFi importando template e inviando dati")
    parser.add_argument("--query", type=str, choices=["1", "2", "3", "4"], help="Esegue direttamente una query")
    parser.add_argument("--mode", type=str, choices=["dataframe", "sql"], default="dataframe", 
                      help="Modalità di esecuzione (default: dataframe)")
    parser.add_argument("--runs", type=int, default=1, help="Numero di esecuzioni (default: 1)")
    
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_args()
    
    try:
        # Converte 'dataframe' a 'no_sql' per compatibilità interna
        mode = "no_sql" if args.mode == "dataframe" else args.mode
        
        nifi_configured = False
        
        # Gestione NiFi
        if args.setup_nifi:
            nifi_configured = setup_nifi()
        
        # Modalità comando diretto
        if args.query:
            execute_query(args.query, mode, args.runs)
        else:
            # Modalità interattiva
            print_header("SABD BATCH PROCESSING")
            main_menu(nifi_configured=args.skip_nifi)
    except KeyboardInterrupt:
        print("\n")
        print_info("Operazione interrotta dall'utente. Arrivederci!")
        sys.exit(0)
    except Exception as e:
        print_error(f"Errore imprevisto: {str(e)}")
        sys.exit(1)