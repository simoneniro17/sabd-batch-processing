import requests
import os
import argparse

# DOCUMENTAZIONE API    --> https://portal.electricitymaps.com/docs/api --> 
# DOWNLOAD DIRETTO      --> https://data.electricitymaps.com/2025-04-03/IT_2021_hourly.csv    

def download_electricity_data(country_code, year, granularity, output_path):
    os.makedirs(output_path, exist_ok=True)
    
    url = f"https://data.electricitymaps.com/2025-04-03/{country_code}_{year}_{granularity}.csv"  
    filename = url.split("/")[-1]
    response = requests.get(url)
    with open(f"{output_path}/{filename}", "wb") as f:
        f.write(response.content)
 
    print(f" - Download dati per {country_code} anno: {year}")

    
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download di dati sull'elettricità da Electricity Maps")
    parser.add_argument("--mode", required=True, choices=["SHORT", "LONG"], help="Modalità: 'SHORT' per IT e SE, 'LONG' per 30 nazioni")
    parser.add_argument("--granularity", default="hourly", choices=["hourly", "daily", "weekly", "monthly", "quarterly", "yearly"], help="Granularità dei dati")
    parser.add_argument("--output", default="./data/raw", help="Percorso della cartella di output")
    args = parser.parse_args()
     # Estensione percorso output in base alla modalità
    final_output_path = os.path.join(args.output, args.mode)

    countries = []
    if args.mode == "SHORT":
        countries = ["IT", "SE"]
        years = ["2021", "2022", "2023", "2024"]
    else:
        countries = [
            "AT", "BE", "FR", "FI", "DE", "GB", "IE", "IT", "NO", "PL",
            "CZ", "SI", "ES", "SE", "CH", "PT", "NL", "SK", "DK", "GR",
            "RO", "BG", "HU", "HR", "EE", "LV", "LT",  # altri paesi UE per arrivare a 27
            "US", "AE", "CN", "IN"  # extra-europei
        ]
        years=["2024"]
    

    for country in countries:
        print(f"\nElaborazione del paese: {country}")
        for year in years:
            download_electricity_data(
                country_code = country,
                year = year,
                granularity = args.granularity,
                output_path = final_output_path,
            )

    print(f"\n\nDownload completato per tutte le zone. Dati salvati in {args.output}")

# Esempio di utilizzo:
# python download_data.py --mode SHORT [--granularity hourly] [--output ./data/raw/SHORT]
# python download_data.py --mode LONG [--granularity hourly] [--output ./data/raw/LONG]
