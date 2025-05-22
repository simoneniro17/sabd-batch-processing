import subprocess
from cli.printer import *
from nifi.functions import feed_nifi_urls


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
        return True
    except Exception as e:
        print_error(f"Errore durante l'invio dati a NiFi: {str(e)}")
        return False
    

def nifi_menu() -> bool:
    """Menu per le operazioni di NiFi. Restituisce True se il template è stato importato."""
    template_imported = False
    
    while True:
        print_header("CONFIGURAZIONE NIFI")
        print("1. Importa template NiFi")
        print("2. Invia dati a NiFi")
        print("3. Setup completo (importa template e invia dati)")
        print("0. Torna al menu principale")
        
        choice = input(f"\n{Colors.BOLD}Scelta [0-3]: {Colors.ENDC}").strip()
        
        if choice == "1":
            template_imported = import_nifi_template()
        elif choice == "2":
            if feed_nifi_data():
                print_success("Dati inviati correttamente a NiFi.")
            else:
                print_warning("Assicurati che il template NiFi sia stato importato prima di inviare i dati.")
        elif choice == "3":
            template_imported = import_nifi_template()
            if template_imported:
                feed_nifi_data()
        elif choice == "0":
            break
        else:
            print_warning("Scelta non valida. Inserisci un numero da 0 a 3.")
            continue
        
        input(f"\n{Colors.BOLD}Premi Invio per continuare...{Colors.ENDC}")
    
    return template_imported