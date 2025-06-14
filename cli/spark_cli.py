from cli.printer import *
from spark.functions import execute_spark_query


def execute_query(query_num: str, mode: str, runs: int = CONFIG["default_runs"]) -> bool:
    """Esegue una query Spark e restituisce True se l'operazione ha successo."""
    print_header(f"ESECUZIONE QUERY {query_num}")
    print_query_info(query_num, mode)
    
    # Costruzione percorso dello script e i parametri
    script_key = f"query{query_num}" if mode == "no_sql" else f"query{query_num}_sql"
    output_path = CONFIG["output_paths"].get(script_key)
    script_path = CONFIG["scripts"].get(script_key)
    
    if not script_path:
        print_error(f"Script non trovato per Query {query_num} in modalità {CONFIG['mode_labels'][mode]}.")
        return False
    
    # Prepara gli input basati sul numero della query
    if query_num in ["1", "3"]:
        input_data = (CONFIG["input_paths"]["it_hourly"], CONFIG["input_paths"]["se_hourly"])
    elif query_num == "2":
        input_data = CONFIG["input_paths"]["it_hourly"]
    elif query_num == "4":
        input_data = CONFIG["input_paths"]["yearly"]
    else:
        print_error(f"Configurazione input non trovata per Query {query_num}.")
        return False
    
    print_info(f"Esecuzione Query {query_num} in modalità {CONFIG['mode_labels'][mode]}...")
    print_info(f"Script: {script_path}")
    print_info(f"Input: {input_data}")
    print_info(f"Output: {output_path}")
    print_info(f"Numero esecuzioni: {runs}")
    
    try:
        execute_spark_query(script_path, input_data, output_path, runs=runs)
        print_success(f"Query {query_num} completata con successo.")
        return True
    except Exception as e:
        print_error(f"Errore durante l'esecuzione della query: {str(e)}")
        return False
    
def execute_all_queries() -> None:
    """Esegue tutte le query in sequenza."""
    runs = input(f"\n{Colors.BOLD}Numero di esecuzioni (per tutte le query) [1]: {Colors.ENDC}").strip()
    runs = int(runs) if runs.isdigit() and int(runs) > 0 else 1

    queries = ["1", "2", "3", "4"]
    modes = [("no_sql", "DataFrame"), ("sql", "SQL")]

    for mode_key, mode_label in modes:
        print_header(f"MODALITÀ {mode_label.upper()}")

        for query_num in queries:
            # Salta la modalità SQL per Query 4
            if query_num == "4" and mode_key == "sql":
                continue

            print_info(f"Esecuzione Query {query_num} - {mode_label}...")
            try:
                success = execute_query(query_num, mode_key, runs)
                if success:
                    print_success(f"Query {query_num} eseguita con successo in modalità {mode_label}.")
                else:
                    print_error(f"Errore nell'esecuzione della Query {query_num} in modalità {mode_label}.")
            except Exception as e:
                print_error(f"Errore durante l'esecuzione della Query {query_num} in modalità {mode_label}: {str(e)}")
            print("-" * 30)

    print_success(f"Esecuzione completata per tutte le query in entrambe le modalità (dove applicabile).")

    
def spark_menu() -> None:
    """Menu per l'esecuzione delle query Spark."""
    while True:
        print_header("MENU QUERY SPARK")
        
        # Mostra le query disponibili
        for q_num, desc in CONFIG["query_descriptions"].items():
            print(f"{q_num}. Query {q_num} - {desc}")
        print("5. Esegui tutte le query sequenzialmente")
        print("0. Torna al menu principale")
        
        choice = input(f"\n{Colors.BOLD}Scelta [0-5]: {Colors.ENDC}").strip()
        
        if choice == "0":
            break

        if choice == "5":
            execute_all_queries()
            input(f"\n{Colors.BOLD}Tutte le query eseguite. Premi Invio per continuare...{Colors.ENDC}")
            continue
        
        if choice not in ["1", "2", "3", "4", "5"]:
            print_warning("Scelta non valida. Inserisci un numero da 0 a 5.")
            continue
        
        # Per Query 4 non c'è SQL
        modes = ["1", "2"] if choice != "4" else ["1"]
        
        while True:
            print("\nModalità di esecuzione:")
            print(f"1. {CONFIG['mode_labels']['no_sql']}")
            if choice != "4":
                print(f"2. {CONFIG['mode_labels']['sql']}")
            print("0. Torna alla selezione query")
            
            mode_choice = input(f"\n{Colors.BOLD}Scelta [0-{len(modes)}]: {Colors.ENDC}").strip()
            
            if mode_choice == "0":
                break
            
            if mode_choice not in modes:
                print_warning(f"Scelta non valida. Inserisci un numero da 0 a {len(modes)}.")
                continue
            
            mode = "no_sql" if mode_choice == "1" else "sql"
            
            runs = input(f"\n{Colors.BOLD}Numero di esecuzioni [1]: {Colors.ENDC}").strip()
            runs = int(runs) if runs.isdigit() and int(runs) > 0 else 1
            
            execute_query(choice, mode, runs)
            
            input(f"\n{Colors.BOLD}Premi Invio per continuare...{Colors.ENDC}")
            break