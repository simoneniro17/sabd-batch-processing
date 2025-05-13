import requests

# Funzione per inviare URL a NiFi (in ascolto su localhost:1406)
def send_url_to_nifi(url, endpoint):
    print(f"Invio URL: {url}")
    response = requests.post(endpoint, data=url)
    
    if response.status_code == 200:
        print(f"Inviato con successo")
    else:
        print(f"Errore nell'invio. - Status: {response.status_code}")
        if hasattr(response, 'text'):
            print(f"Risposta errore: {response.text[:100]}")

# 
def feed_nifi_urls(granularity="hourly", nifi_endpoint="http://localhost:1406/contentListener"):
    """
    Genera gli URL dei dataset di Electricity Maps da inviare a NiFi. Li invia chiamando la funzione `send_url_to_nifi`.

    Args:
        granularity (str): Livello di granularità del dataset ['hourly', 'daily', 'monthly', 'yearly']. Default è "hourly".
        nifi_endpoint (str): Endpoint HTTP di NiFi a cui inviare gli URL. Default è "http://localhost:1406/contentListener".
    """

    short_countries = ["IT", "SE"]
    short_years = ["2021", "2022", "2023", "2024"]
    
    long_countries = [
        # Paesi europei
        # Austria, Belgio, Francia, Finlandia, Germania, Gran Bretagna, Irlanda, Italia, Norvegia, Polonia,
        # Repubblica Ceca, Slovenia, Spagna, Svezia, Svizzera
        "AT", "BE", "FR", "FI", "DE", "GB", "IE", "IT", "NO", "PL",
        "CZ", "SI", "ES", "SE", "CH",

        # Paesi extra-europei
        # Stati Uniti, Emirati Arabi Uniti, Cina, India, Giappone, Brasile, Messico, Canada, Australia, Sudafrica,
        # Corea del Sud, Indonesia, Singapore, Argentina, Egitto
        "US", "AE", "CN", "IN", "JP", "BR", "MX", "CA", "AU", "ZA",
        "KR", "ID", "SG", "AR", "EG"
    ]
    long_years = ["2024"]

    # Paesi con tutti gli anni e granularità oraria
    for country in short_countries:
        for year in short_years:
            url = f"https://data.electricitymaps.com/2025-04-03/{country}_{year}_{granularity}.csv"
            send_url_to_nifi(url, nifi_endpoint)
    
    # Paesi solo con il 2024 e granularità annuale
    for country in long_countries:
        for year in long_years:
            url = f"https://data.electricitymaps.com/2025-04-03/{country}_{year}_yearly.csv"
            send_url_to_nifi(url, nifi_endpoint)
