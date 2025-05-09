import requests
import os
import json
import argparse

# https://portal.electricitymaps.com/docs/api

#https://data.electricitymaps.com/2025-04-03/IT-SIC_2021_hourly.csv

def download_electricity_data(zone_code, year, granularity, output_path):
    os.makedirs(output_path, exist_ok=True) # Creiamo la cartella di output se non esiste
    url = f"https://data.electricitymaps.com/2025-04-03/{zone_code}_{year}_{granularity}.csv"  
    filename =  url.split("/")[-1]
    r = requests.get(url)
    with open(f"./data/raw/{filename}", "wb") as f:
        f.write(r.content)
 
    print(f"Download dati per {zone_code} anno:{year}")

    
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download di dati sull'elettricità da Electricity Maps")
    parser.add_argument("--country", required=True, choices=["IT", "SE"], help="Paese: 'IT' per Italia, 'SE' per Svezia")
    parser.add_argument("--year", required=False, help="Anno per il download dei dati")
    parser.add_argument("--granularity", required=False, choices=["hourly", "daily", "weekly", "monthly", "quarterly", "yearly"], help="Granularità dei dati")
    parser.add_argument("--output", default="./data/raw", help="Percorso della cartella di output")


    args = parser.parse_args()

    zones = {
        "IT": ["IT-CNO", "IT-CSO", "IT-NO", "IT-SAR", "IT-SIC", "IT-SO"],
        "SE": ["SE-SE1", "SE-SE2", "SE-SE3", "SE-SE4"]
    }
    years = ["2021", "2022", "2023", "2024"]
    granularity= ["hourly", "daily", "weekly ", "monthly","quarterly", "yearly"]

    for zone in zones[args.country]:
        for year in years:
            download_electricity_data(
                zone_code=zone,
                year=year,
                output_path=args.output,
                granularity=granularity[0]
            )
    print(f"Download completato per tutte le zone di {args.country}. Dati salvati in {args.output}")

# python download_data.py --token <token> --country IT --start 2021-01-01 --end 2024-12-31


# response = requests.get(
    #     "https://api.electricitymap.org/v3/power-breakdown/history",
    #     headers={"auth-token": token}, 
    #     params={
    #         "zone": zone_code,
    #         # "start": f"{start_date}T00:00:00Z",
    #         # "end": f"{end_date}T23:00:00Z",
    #         "aggregationPeriod": "hourly"}
    # )
    

    # if response.status_code == 200:
    #     data = response.json()

    #     filename = f"{zone_code}_electricity_data_{start_date}_to_{end_date}.json"
    #     filepath = os.path.join(output_path, filename)
    #     with open(filepath, "w") as f:
    #         json.dump(data, f, indent=2)
    #         print(f"Dati scaricati e salvati in {filepath}")
    # else:
    #         print(f"Errore {response.status_code} durante il download dei dati per {zone_code}: {response.text}")

    # return filepath