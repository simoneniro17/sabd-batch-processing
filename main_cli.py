import sys
import argparse

from cli.constants import CONFIG, Colors
from cli.printer import *
from cli.helper import show_help
from cli.nifi_cli import *
from cli.spark_cli import *
from cli.redis_cli import *


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
            print_info("Esco dal programma...")
            break
        else:
            print_warning("Scelta non valida. Inserisci un numero da 1 a 5.")


def parse_args():
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
        print_info("Operazione interrotta dall'utente.")
        sys.exit(0)
    except Exception as e:
        print_error(f"Errore imprevisto: {str(e)}")
        sys.exit(1)