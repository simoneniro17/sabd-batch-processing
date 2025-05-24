import os
import subprocess
from cli.printer import *
from cli.constants import CONFIG
from redis.functions import load_to_redis, export_from_redis


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
        host_results_dir = CONFIG["host_results_dir"]
        os.makedirs(host_results_dir, exist_ok=True)

        local_path = CONFIG["redis"]["local_path"]
        print_info(f"Download dati da Redis in locale...")
        export_from_redis(local_path)
        
        print_success("Dati scaricati da Redis con successo.")
        print_info(f"I file sono stati salvati in {host_results_dir} in locale.")
        return True
    except Exception as e:
        print_error(f"Errore durante il download da Redis: {str(e)}")
        return False
    

def redis_reset() -> bool:
    """Resetta completamente Redis eliminando tutte le chiavi."""
    print_header("RESET REDIS")
    print_warning("Questa operazione eliminerà TUTTI i dati da Redis.")
    
    confirm = input(f"\n{Colors.YELLOW}Continua? (s/n): {Colors.ENDC}").strip().lower()
    if confirm not in ['s', 'si', 'sì', 'y', 'yes']:
        print_info("Annullato.")
        return False
    
    try:
        print_info("Resettando Redis...")
        result = subprocess.run(
            "docker exec redis redis-cli FLUSHALL",
            shell=True,
            capture_output=True,
            text=True
        )
        
        if result.returncode == 0:
            print_success("Redis resettato con successo.")
            return True
        else:
            print_error(f"Errore nel reset di Redis: {result.stderr}")
            return False
            
    except Exception as e:
        print_error(f"Errore: {str(e)}")
        return False


def redis_status() -> None:
    """Mostra informazioni su Redis."""
    print_header("STATO REDIS")
    
    try:
        # Numero di chiavi
        result = subprocess.run(
            "docker exec redis redis-cli DBSIZE",
            shell=True,
            capture_output=True,
            text=True
        )
        
        if result.returncode == 0:
            num_keys = result.stdout.strip()
            print_info(f"Numero di chiavi in Redis: {num_keys}")
        else:
            print_warning("Impossibile recuperare il numero di chiavi")
    
    except Exception as e:
        print_error(f"Errore: {str(e)}")

    
def redis_menu() -> None:
    """Menu per le operazioni su Redis."""
    while True:
        print_header("MENU REDIS")
        print("1. Carica dati su Redis (da HDFS)")
        print("2. Scarica dati da Redis (in locale)")
        print("3. Mostra stato Redis (numero di chiavi)")
        print("4. Reset completo Redis")
        print("0. Torna al menu principale")
        
        choice = input(f"\n{Colors.BOLD}Scelta [0-4]: {Colors.ENDC}").strip()
        
        if choice == "1":
            redis_upload()
        elif choice == "2":
            redis_download()
        elif choice == "3":
            redis_status()
        elif choice == "4":
            redis_reset()
        elif choice == "0":
            break
        else:
            print_warning("Scelta non valida. Inserisci un numero da 0 a 4.")
            continue
        
        input(f"\n{Colors.BOLD}Premi Invio per continuare...{Colors.ENDC}")