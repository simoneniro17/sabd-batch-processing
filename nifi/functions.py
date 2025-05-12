import requests

def send_url_to_nifi(url, endpoint):
    print(f"Invio URL: {url}")
    response = requests.post(endpoint, data=url)
    
    if response.status_code == 200:
        print(f"Inviato con successo")
    else:
        print(f"Errore nell'invio. - Status: {response.status_code}")
        if hasattr(response, 'text'):
            print(f"Risposta errore: {response.text[:100]}")

def feed_nifi_urls(mode, granularity="hourly", nifi_endpoint="http://localhost:1406/contentListener"):
    """
    Invia URL di dataset di Electricity Maps a NiFi.

    Args:
        mode (str): 'SHORT' (IT e SE) oppure 'LONG' (30 paesi)
        granularity (str): Una delle granularità supportate: 'hourly', 'daily', etc.
        nifi_endpoint (str): Endpoint HTTP di NiFi

    Example:
        feed_nifi_urls("SHORT")
        feed_nifi_urls("LONG", granularity="daily", nifi_endpoint="http://nifi:1406/contentListener")
    """
    if mode == "SHORT":
        countries = ["IT", "SE"]
        years = ["2021", "2022", "2023", "2024"]
    elif mode == "LONG":
        countries = [
            "AT", "BE", "FR", "FI", "DE", "GB", "IE", "IT", "NO", "PL",
            "CZ", "SI", "ES", "SE", "CH", "PT", "NL", "SK", "DK", "GR",
            "RO", "BG", "HU", "HR", "EE", "LV", "LT",
            "US", "AE", "CN", "IN"
        ]
        years = ["2024"]
    else:
        raise ValueError("Modalità non supportata: usa 'SHORT' o 'LONG'.")

    for country in countries:
        for year in years:
            url = f"https://data.electricitymaps.com/2025-04-03/{country}_{year}_{granularity}.csv"
            send_url_to_nifi(url, nifi_endpoint)
