import sys
import argparse

from cli.constants import CONFIG, Colors
from cli.printer import *
from cli.helper import *
from cli.nifi_cli import *
from cli.spark_cli import *
from cli.redis_cli import *
from cli.hdfs_cli import *
from cli.docker_utils import *


def main_menu() -> None:
    """Menu principale dell'applicazione."""    
    while True:
        print_header("MENU PRINCIPALE")
        print("1. Esegui Query Spark")
        print("2. Gestisci Redis")
        print("3. Configura NiFi")
        print("4. Gestisci HDFS")
        print("5. Gestisci Container Docker")
        print("6. Help / Informazioni")
        print("0. Esci")
        
        choice = input(f"\n{Colors.BOLD}Scelta [0-6]: {Colors.ENDC}").strip()
        
        if choice == "1":
            spark_menu()
        elif choice == "2":
            redis_menu()
        elif choice == "3":
            nifi_menu()
        elif choice == "4":
            hdfs_menu()
        elif choice == "5":
            docker_menu()
        elif choice == "6":
            show_help()
            input(f"\n{Colors.BOLD}Premi Invio per continuare...{Colors.ENDC}")
        elif choice == "0":
            shutdown, remove = ask_docker_shutdown()
            if shutdown:
                if remove:
                    volumes = input(f"{Colors.YELLOW}Vuoi rimuovere anche i volumi? (s/n): {Colors.ENDC}").strip().lower() in ['s', 'si', 'sì', 'y', 'yes']
                    docker_compose_down(remove_volumes=volumes)
                else:
                    docker_compose_stop()
            print_info("Esco dal programma...")
            break
        else:
            print_warning("Scelta non valida. Inserisci un numero da 0 a 6.")


def parse_args():
    parser = argparse.ArgumentParser(description="SABD Batch Processing CLI")
    parser.add_argument("--skip-nifi", action="store_true", help="Salta l'inizializzazione di NiFi")
    parser.add_argument("--setup-nifi", action="store_true", help="Configura NiFi importando template e inviando dati")
    parser.add_argument("--query", type=str, choices=["1", "2", "3", "4"], help="Esegue direttamente una query")
    parser.add_argument("--mode", type=str, choices=["dataframe", "sql"], default="dataframe", 
                      help="Modalità di esecuzione (default: dataframe)")
    parser.add_argument("--runs", type=int, default=1, help="Numero di esecuzioni (default: 1)")
    parser.add_argument("--start-docker", action="store_true", help="Avvia i container Docker all'inizio")
    parser.add_argument("--reset-hdfs", action="store_true", help="Resetta il contenuto di HDFS")

    return parser.parse_args()

if __name__ == "__main__":
    args = parse_args()
    
    try:
        # Avvio Docker se richiesto
        if args.start_docker:
            docker_started = docker_compose_up()
            if not docker_started:
                print_warning("I container Docker non sono stati avviati correttamente.")
                if not ask_continue():
                    sys.exit(1)

        # Reset HDFS se richiesto
        if args.reset_hdfs:
            hdfs_reset()
            if not args.query:  # Se non ci sono altre operazioni, esci
                sys.exit(0)    
        
        # Converte 'dataframe' a 'no_sql' per compatibilità interna
        mode = "no_sql" if args.mode == "dataframe" else args.mode
        
        nifi_configured = False
        
        # Gestione NiFi
        if args.setup_nifi:
            # Usa l'importazione del template e feed data direttamente
            nifi_configured = import_nifi_template()
            if nifi_configured:
                feed_nifi_data()
        
        # Modalità comando diretto
        if args.query:
            execute_query(args.query, mode, args.runs)
        else:
            # Modalità interattiva
            print_header("SABD BATCH PROCESSING")
            main_menu()
    except KeyboardInterrupt:
        print("\n")
        print_info("Applicazione interrotta dall'utente.")
        sys.exit(0)
    except Exception as e:
        print_error(f"Errore imprevisto: {str(e)}")
        sys.exit(1)
