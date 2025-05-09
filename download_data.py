import requests
import os
import json
import argparse
import time
from datetime import datetime, timedelta

def download_electricity_data(country, start_date, end_date, output_path):
    
    os.makedirs(output_path, exist_ok=True) # Creiamo la cartella di output se non esiste

    # Convertiamo le date in oggetti datetime
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")

    # Electrycity Maps API URL ## TODO
    base_url = "https://api.electricitymaps.com/v3/"

    filename = f"{country}_electricity_data_{start_date}_to_{end_date}.json"
    filepath = os.path.join(output_path, filename)
    if os.path.exists(filepath):
        print(f"Il file {filepath} esiste già, non verrà scaricato di nuovo.")
        return filepath
    
    # Creiamo l'header CSV
    header = "timestamp," \
    "carbon_intensity," \
    "carbon_free_energy_share," \
    "renewable_energy_share\n"

    with open(filepath, "w") as f:
        f.write(header)

        # Iteriamo sulle date
        current_date = start
        while current_date <= end:
            try:
                # Formattiamo i paramemtri per la richiesta API
                params = {
                    "zone": country,
                    "date": current_date.strftime("%Y-%m-%d"),
                    "hourly": "true",
                }

                # Chiave API
                headers = {
                    "Authorization": "ciao" ## TODO forse non serve, dobbiamo capire se 
                }

                # Effettuiamo la richiesta GET
                print(f"Download dati per {country} in data {current_date.strftime('%Y-%m-%d')}")
                response = requests.get(base_url, params=params, headers=headers)

                if response.status_code == 200:
                    data = response.json()

                    # Processiamo i dati orari
                    for hour_data in data["data"]:
                        timestamp = hour_data["timestamp"]
                        carbon_intensity = hour_data.get("carbonIntensity", "")
                        carbon_free_energy_share = hour_data.get("carbonFreeEnergyShare", "")
                        renewable_energy_share = hour_data.get("renewableEnergyShare", "")

                        # Scriviamo i dati nel file CSV
                        f.write(f"{timestamp},{carbon_intensity},{carbon_free_energy_share},{renewable_energy_share}\n")
                else:
                    print(f"Errore durante il download dei dati per {country} in data {current_date.strftime('%Y-%m-%d')}: {response.status_code}")

            except Exception as e:
                print(f"Si è verificato un errore durante il download dei dati per {country} in data {current_date.strftime('%Y-%m-%d')}: {e}")

            current_date += timedelta(days=1)

            time.sleep(1)  # Aggiungiamo un ritardo di 1 secondo tra le richieste per evitare di sovraccaricare il server

    return filepath
    
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download di dati sull'elettricità da Electricity Maps")
    parser.add_argument("--country", required=True, help="Nome del paese ('IT' per Italia, 'SE' per Svezia")
    parser.add_argument("--start", required=True, help="Data di inizio (YYYY-MM-DD)")
    parser.add_argument("--end", required=True, help="Data di fine (YYYY-MM-DD)")
    parser.add_argument("--output", default="./data/raw", help="Percorso della cartella di output")

    args = parser.parse_args()

    download_electricity_data(args.country, args.start, args.end, args.output)
    print(f"Dati scaricati e salvati in {args.output}")
