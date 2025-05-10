import requests
import os
import json
import argparse

# https://portal.electricitymaps.com/docs/api
# https://data.electricitymaps.com/2025-04-03/IT-SIC_2021_hourly.csv

def download_electricity_data(zone_code, year, granularity, output_path):
    os.makedirs(output_path, exist_ok=True)
    
    url = f"https://data.electricitymaps.com/2025-04-03/{zone_code}_{year}_{granularity}.csv"  
    filename =  url.split("/")[-1]
    response = requests.get(url)
    with open(f"./data/raw/{filename}", "wb") as f:
        f.write(response.content)
 
    print(f"Download dati per {zone_code} anno: {year}")

    
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download di dati sull'elettricità da Electricity Maps")
    parser.add_argument("--country", required=True, choices=["IT", "SE"], help="Paese: 'IT' per Italia, 'SE' per Svezia")
    parser.add_argument("--year", help="Anno per il download dei dati")
    parser.add_argument("--granularity", default="hourly", choices=["hourly", "daily", "weekly", "monthly", "quarterly", "yearly"], help="Granularità dei dati")
    parser.add_argument("--output", default="./data/raw", help="Percorso della cartella di output")
    args = parser.parse_args()

    zones = {
        "IT": ["IT-CNO", "IT-CSO", "IT-NO", "IT-SAR", "IT-SIC", "IT-SO"],
        "SE": ["SE-SE1", "SE-SE2", "SE-SE3", "SE-SE4"]
    }
    years = ["2021", "2022", "2023", "2024"]
    granularity = ["hourly", "daily", "weekly", "monthly", "quarterly", "yearly"]

    for zone in zones[args.country]:
        for year in years:
            download_electricity_data(
                zone_code = zone,
                year = year,
                granularity = args.granularity,
                output_path = args.output,
            )

    print(f"Download completato per tutte le zone di {args.country}. Dati salvati in {args.output}")

# download_data.py --country <COUNTRY> [--granularity <GRANULARITY>] [--output <OUTPUT>]