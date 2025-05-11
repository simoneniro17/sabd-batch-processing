import requests
import argparse

# DOCUMENTAZIONE API    --> https://portal.electricitymaps.com/docs/api --> 
# DOWNLOAD DIRETTO      --> https://data.electricitymaps.com/2025-04-03/IT-SIC_2021_hourly.csv    

def send_url_to_nifi(url, endpoint):
    print(f"Invio URL: {url}")
    response = requests.post(endpoint, data=url,)
    
    if response.status_code == 200:
        print(f"Inviato con successo")
    else:
        print(f"Errore nell'invio. - Status: {response.status_code}")
        if hasattr(response, 'text'):
            print(f"Risposta errore: {response.text[:100]}")

    
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Invio degli URL dei dataset di Electricity Maps a NiFi")
    parser.add_argument("--mode", required=True, choices=["SHORT", "LONG"], help="Modalità: 'SHORT' per IT e SE, 'LONG' per 30 paesi")
    parser.add_argument("--granularity", default="hourly", choices=["hourly", "daily", "weekly", "monthly", "quarterly", "yearly"], help="Granularità dei dati")
    parser.add_argument("--nifi-endpoint", default="http://localhost:1406/contentListener", help="Endpoint NiFi per l'invio degli URL")
    args = parser.parse_args()

    if args.mode == "SHORT":
        countries = ["IT", "SE"]
        years = ["2021", "2022", "2023", "2024"]
    else:
        countries = [
            "AT", "BE", "FR", "FI", "DE", "GB", "IE", "IT", "NO", "PL",
            "CZ", "SI", "ES", "SE", "CH", "PT", "NL", "SK", "DK", "GR",
            "RO", "BG", "HU", "HR", "EE", "LV", "LT",
            "US", "AE", "CN", "IN"
        ]
        years = ["2024"]

    for country in countries:
        for year in years:
            url = f"https://data.electricitymaps.com/2025-04-03/{country}_{year}_{args.granularity}.csv"
            send_url_to_nifi(url, args.nifi_endpoint)

# nifi_url_feeder.py --mode  <LONG/SHORT> [--granularity <GRANULARITY>]