from cli.constants import CONFIG, Colors

def print_header(text: str):
    print(f"\n{Colors.HEADER}{Colors.BOLD}{'=' * 60}{Colors.ENDC}")
    print(f"{Colors.HEADER}{Colors.BOLD}{text.center(60)}{Colors.ENDC}")
    print(f"{Colors.HEADER}{Colors.BOLD}{'=' * 60}{Colors.ENDC}\n")

def print_info(text: str):
    """Stampa un messaggio informativo."""
    print(f"{Colors.BLUE}ℹ {text}{Colors.ENDC}")

def print_success(text: str):
    """Stampa un messaggio di successo."""
    print(f"{Colors.GREEN}✓ {text}{Colors.ENDC}")

def print_warning(text: str):
    """Stampa un avviso."""
    print(f"{Colors.YELLOW}⚠ {text}{Colors.ENDC}")

def print_error(text: str):
    """Stampa un messaggio di errore."""
    print(f"{Colors.RED}✗ {text}{Colors.ENDC}")

def print_query_info(query_num: str, mode: str):
    """Stampa le informazioni sulla query selezionata."""
    mode_label = CONFIG["mode_labels"][mode]
    query_desc = CONFIG["query_descriptions"][query_num]
    
    print(f"\n{Colors.CYAN}{Colors.BOLD}Query {query_num}: {query_desc}{Colors.ENDC}")
    print(f"{Colors.CYAN}Modalità: {mode_label}{Colors.ENDC}")
