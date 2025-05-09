import requests
import os
import json
import argparse

# https://portal.electricitymaps.com/docs/api

def download_electricity_data(token, zone_code, start_date, end_date, output_path):
    
    os.makedirs(output_path, exist_ok=True) # Creiamo la cartella di output se non esiste

    # Electrycity Maps API URL, Token e parametri ## TODO
    base_url = "https://api.electricitymaps.com/v3/power-breakdown/history"
    headers = {"auth-token": token}
    params = {
        "zone": zone_code,
        "start": f"{start_date}T00:00:00Z",
        "end": f"{end_date}T23:00:00Z",
        "interval": "hour",
    }

    # Effettuiamo la richiesta GET
    print(f"Download dati per {zone_code} da {start_date} a {end_date}")
    response = requests.get(base_url, params=params, headers=headers)
    
    if response.status_code == 200:
        data = response.json()

        filename = f"{zone_code}_electricity_data_{start_date}_to_{end_date}.json"
        filepath = os.path.join(output_path, filename)
        with open(filepath, "w") as f:
            json.dump(data, f, indent=2)
            print(f"Dati scaricati e salvati in {filepath}")
    else:
            print(f"Errore {response.status_code} durante il download dei dati per {zone_code}: {response.text}")

    return filepath
    
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download di dati sull'elettricità da Electricity Maps")
    parser.add_argument("--country", required=True, choices=["IT", "SE"], help="Paese: 'IT' per Italia, 'SE' per Svezia")
    parser.add_argument("--start", required=True, help="Data di inizio (YYYY-MM-DD)")
    parser.add_argument("--end", required=True, help="Data di fine (YYYY-MM-DD)")
    parser.add_argument("--output", default="./data/raw", help="Percorso della cartella di output")
    parser.add_argument("--token", help="API token (o variabile d'ambiente ELECTRICITYMAP_TOKEN)")

    args = parser.parse_args()

    if not args.token:
        from dotenv import load_dotenv
        load_dotenv()
        args.token = os.getenv("ELECTRICITYMAP_TOKEN")

    if not args.token:
        raise ValueError("Token non fornito. Usa --token oppure imposta la variabile d'ambiente ELECTRICITYMAP_TOKEN.")

    zones = {
        "IT": ["IT-CNO", "IT-CSO", "IT-NO", "IT-SAR", "IT-SIC", "IT-SO"],
        "SE": ["SE-SE1", "SE-SE2", "SE-SE3", "SE-SE4"]
    }

    for zone in zones[args.country]:
        download_electricity_data(
            token=args.token,
            zone_code=zone,
            start_date=args.start,
            end_date=args.end,
            output_path=args.output
        )

    print("Download completato per tutte le zone di {args.country}. Dati salvati in {args.output}")

# python download_data.py --token <token> --country IT --start 2021-01-01 --end 2024-12-31