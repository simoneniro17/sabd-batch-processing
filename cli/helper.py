from cli.printer import *
from cli.constants import Colors

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