from cli.printer import *
from cli.constants import Colors

def show_help() -> None:
    """Mostra informazioni di aiuto sull'applicazione."""
    print_header("INFORMAZIONI SUL PROGETTO")
    print("""
Questo strumento consente di eseguire l'intero workflow del progetto SABD:
    """)
    
    print_header("COMANDI DISPONIBILI")
    
    commands = [
        ("1", "Esegui Query Spark", "Esegue una delle query implementate (1-4)"),
        ("2", "Gestisci Redis", "Carica o scarica dati in e da Redis"),
        ("3", "Gestisci NiFi", "Importa template e invia dati a NiFi"),
        ("4", "Gestisci HDFS", "Mostra lo stato e il contenuto di HDFS, con l'opzione di resettarlo"),
        ("5", "Gestisci Docker", "Avvia o arresta i container Docker"),
        ("6", "Help", "Mostra questa guida"),
        ("0", "Esci", "Termina l'applicazione")
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
  --start-docker     Avvia i container Docker all'inizio
  --reset-hdfs       Resetta il contenuto di HDFS
""")

def ask_continue() -> bool:
    """Chiede all'utente se vuole continuare nonostante gli errori."""
    choice = input(f"\n{Colors.YELLOW}Continuare comunque? (s/n): {Colors.ENDC}").strip().lower()
    return choice in ['s', 'si', 'sì', 'y', 'yes']