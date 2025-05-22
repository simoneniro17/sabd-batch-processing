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
        local_path = CONFIG["redis"]["local_path"]
        print_info(f"Download dati da Redis a {local_path}...")
        export_from_redis(local_path)
        print_success("Dati scaricati da Redis con successo.")
        print_info(f"I file sono stati salvati in: {local_path}")
        return True
    except Exception as e:
        print_error(f"Errore durante il download da Redis: {str(e)}")
        return False
    
    
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