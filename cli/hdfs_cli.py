import subprocess
from cli.printer import *


def hdfs_reset() -> bool:
    """Esegue reset.sh e riavvia HDFS per applicare le modifiche."""
    print_header("RESET HDFS")
    print_warning("Questa operazione resetterà completamente HDFS.")
    
    confirm = input(f"\n{Colors.YELLOW}Continua? (s/n): {Colors.ENDC}").strip().lower()
    if confirm not in ['s', 'si', 'sì', 'y', 'yes']:
        print_info("Annullato.")
        return False
    
    try:
        print_info("Eseguendo reset.sh...")
        result = subprocess.run(f'bash ./reset.sh', cwd="hdfs", shell=True)
        
        if result.returncode != 0:
            print_error("Errore nel reset")
            return False
        print_success("Reset completato")
        
        hdfs_containers = ["namenode", "datanode1", "datanode2"]
        print_info("Riavviando container HDFS...")
        for container in hdfs_containers:
            subprocess.run(f"docker restart {container}", shell=True, capture_output=True)
        print_success("Container riavviati")
        print_success("HDFS resettato e riavviato!")
        
        return True
        
    except Exception as e:
        print_error(f"Errore: {str(e)}")
        return False


def hdfs_status() -> None:
    """Mostra contenuto HDFS."""
    print_header("STATO HDFS")
    
    try:
        result = subprocess.run(
            "docker exec namenode hdfs dfs -ls -R /",
            shell=True, capture_output=True, text=True
        )
        
        if result.returncode == 0:
            print(result.stdout if result.stdout.strip() else "HDFS vuoto")
        else:
            print_warning("HDFS non raggiungibile")
            
    except Exception as e:
        print_error(f"Errore: {str(e)}")


def hdfs_menu() -> None:
    """Menu HDFS."""
    while True:
        print_header("GESTIONE HDFS")
        print("1. Mostra contenuto")
        print("2. Reset completo")
        print("0. Indietro")
        
        choice = input(f"\n{Colors.BOLD}Scelta: {Colors.ENDC}").strip()
        
        if choice == "1":
            hdfs_status()
        elif choice == "2":
            hdfs_reset()
        elif choice == "0":
            break
        else:
            print_warning("Scelta non valida.")
            continue
        
        input(f"\n{Colors.BOLD}Premi Invio...{Colors.ENDC}")