import requests
import argparse

# DOCUMENTAZIONE API    --> https://portal.electricitymaps.com/docs/api --> 
# DOWNLOAD DIRETTO      --> https://data.electricitymaps.com/2025-04-03/IT-SIC_2021_hourly.csv    

def send_url_to_nifi(url, endpoint):
    print(f"Invio URL: {url}")
    response = requests.post(endpoint, data=url)
    
    if response.status_code == 200:
        print(f"Inviato con successo")
    else:
        print(f"Errore nell'invio. - Status: {response.status_code}")
        if hasattr(response, 'text'):
            print(f"Risposta errore: {response.text[:100]}")

    
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Invio degli URL dei dataset di Electricity Maps a NiFi")
    parser.add_argument("--country", required=True, choices=["IT", "SE"], help="Paese per il download dei dati")
    parser.add_argument("--granularity", default="hourly", choices=["hourly", "daily", "weekly", "monthly", "quarterly", "yearly"], help="Granularità dei dati")
    parser.add_argument("--nifi-endpoint", default="http://localhost:1406/contentListener", help="Endpoint NiFi per l'invio degli URL")
    args = parser.parse_args()

    zones = {
        "IT": ["IT-CNO", "IT-CSO", "IT-NO", "IT-SAR", "IT-SIC", "IT-SO"],
        "SE": ["SE-SE1", "SE-SE2", "SE-SE3", "SE-SE4"]
    }
    years = ["2021", "2022", "2023", "2024"]

    for zone in zones[args.country]:
        for year in years:
            url = f"https://data.electricitymaps.com/2025-04-03/{zone}_{year}_{args.granularity}.csv"
            send_url_to_nifi(url, args.nifi_endpoint)

# nifi_url_feeder.py --country <COUNTRY> [--granularity <GRANULARITY>]