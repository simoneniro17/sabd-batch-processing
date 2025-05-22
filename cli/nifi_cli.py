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