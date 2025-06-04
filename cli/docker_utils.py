import subprocess
from cli.printer import *


def docker_compose_up() -> bool:
    """Avvia i container Docker utilizzando docker-compose."""
    print_header("AVVIO CONTAINER DOCKER")
    print_info("Avvio dei container in corso...")
    print_warning("Se è la prima volta, potrebbe essere necessario scaricare le immagini Docker, il che potrebbe richiedere del tempo.")
    
    try:
        result = subprocess.run(
            "docker-compose up -d",
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        
        if result.returncode == 0:
            print_success("Container Docker avviati con successo.")
            return True
        else:
            print_error(f"Errore durante l'avvio dei container (codice {result.returncode}).")
            print_warning("Assicurarsi che il Docker Engine sia in esecuzione.")
            print_info("Dettagli errore:")
            for line in result.stderr.splitlines():
                print(f"  {line}")
            return False
    except Exception as e:
        print_error(f"Eccezione durante l'avvio dei container Docker: {str(e)}")
        print_warning("Assicurarsi che Dcoker Engine sia in esecuzione.")
        return False


def docker_compose_stop() -> bool:
    """Arresta i container Docker senza rimuoverli."""
    print_header("ARRESTO CONTAINER DOCKER")
    print_info("Arresto dei container in corso (senza rimozione)...")

    try:
        result = subprocess.run(
            "docker-compose stop",
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        
        if result.returncode == 0:
            print_success("Container Docker arrestati con successo.")
            return True
        else:
            print_error(f"Errore durante l'arresto dei container (codice {result.returncode}).")
            print_info("Dettagli errore:")
            for line in result.stderr.splitlines():
                print(f"  {line}")
            return False
    except Exception as e:
        print_error(f"Eccezione durante l'arresto dei container Docker: {str(e)}")
        return False
    
def docker_compose_down(remove_volumes: bool = False) -> bool:
    """Arresta e rimuove i container Docker.
    """
    print_header("RIMOZIONE CONTAINER DOCKER")
    
    command = "docker-compose down -v" if remove_volumes else "docker-compose down"
    
    action_desc = "con rimozione dei volumi" if remove_volumes else "senza rimozione dei volumi"
    print_info(f"Arresto e rimozione dei container in corso ({action_desc})...")
    
    try:
        result = subprocess.run(
            command,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        
        if result.returncode == 0:
            print_success("Container Docker rimossi con successo.")
            return True
        else:
            print_error(f"Errore durante la rimozione dei container (codice {result.returncode}).")
            print_info("Dettagli errore:")
            for line in result.stderr.splitlines():
                print(f"  {line}")
            return False
    except Exception as e:
        print_error(f"Eccezione durante la rimozione dei container Docker: {str(e)}")
        return False


def docker_ps() -> None:
    """Mostra i container Docker in esecuzione."""
    print_header("CONTAINER DOCKER IN ESECUZIONE")
    
    try:
        result = subprocess.run(
            "docker ps",
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        
        if result.returncode == 0:
            if result.stdout.strip():
                print(result.stdout)
            else:
                print_info("Non ci sono container in esecuzione.")
        else:
            print_error(f"Errore nell'esecuzione di docker ps (codice {result.returncode}).")
            print_info("Dettagli errore:")
            for line in result.stderr.splitlines():
                print(f"  {line}")
    except Exception as e:
        print_error(f"Eccezione durante l'esecuzione di docker ps: {str(e)}")

    
def ask_docker_shutdown() -> bool:
    """Chiede all'utente se vuole fermare i container Docker prima di uscire."""
    print_header("GESTIONE CONTAINER DOCKER ALL'USCITA")
    print("Prima di uscire, come vuoi gestire i container Docker?")
    print("1. Non fare nulla")
    print("2. Arresta i container (docker-compose stop)")
    print("3. Rimuovi i container (docker-compose down)")
    print("4. Rimuovi i container e i volumi (docker-compose down -v)")
    
    choice = input(f"\n{Colors.BOLD}Scelta [1-4]: {Colors.ENDC}").strip()
    
    if choice == "1":
        return False, False
    elif choice == "2":
        return True, False
    elif choice == "3":
        return True, True
    elif choice == "4":
        print_warning("Questa operazione eliminerà tutti i dati nei volumi Docker.")
        confirm = input(f"{Colors.YELLOW}Sei sicuro di voler continuare? (y/n): {Colors.ENDC}").strip().lower()
        if confirm in ['s', 'si', 'sì', 'y', 'yes']:
            return True, True
        else:
            print_info("I container rimarranno in esecuzione.")
            return False, False
    else:
        print_warning("Scelta non valida. I container rimarranno in esecuzione.")
        return False, False


def docker_menu() -> None:
    """Menu per le operazioni di gestione Docker."""
    while True:
        print_header("GESTIONE CONTAINER DOCKER")
        print("1. Avvia tutti i container (docker-compose up)")
        print("2. Arresta i container (docker-compose stop)")
        print("3. Rimuovi i container (docker-compose down)")
        print("4. Rimuovi i container e i volumi (docker-compose down -v)")
        print("5. Mostra i container in esecuzione (docker ps)")
        print("0. Torna al menu principale")
        
        choice = input(f"\n{Colors.BOLD}Scelta [0-5]: {Colors.ENDC}").strip()
        
        if choice == "1":
            docker_compose_up()
        elif choice == "2":
            docker_compose_stop()
        elif choice == "3":
            docker_compose_down(remove_volumes=False)
        elif choice == "4":
            print_warning("Questa operazione eliminerà tutti i dati nei volumi Docker.")
            confirm = input(f"{Colors.YELLOW}Sei sicuro di voler continuare? (s/n): {Colors.ENDC}").strip().lower()
            if confirm in ['s', 'si', 'sì', 'y', 'yes']:
                docker_compose_down(remove_volumes=True)
            else:
                print_info("Operazione annullata.")
        elif choice == "5":
            docker_ps()
        elif choice == "0":
            break
        else:
            print_warning("Scelta non valida. Inserisci un numero da 0 a 5.")
            continue
        
        input(f"\n{Colors.BOLD}Premi Invio per continuare...{Colors.ENDC}")

