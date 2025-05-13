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

def feed_nifi_urls(granularity="hourly", nifi_endpoint="http://localhost:1406/contentListener"):
    """
    Invia URL di dataset di Electricity Maps a NiFi.

    Args:
        granularity (str): Una delle granularità supportate: 'hourly', 'daily', etc.
        nifi_endpoint (str): Endpoint HTTP di NiFi

    Example:
        feed_nifi_urls()
        feed_nifi_urls(granularity="daily", nifi_endpoint="http://nifi:1406/contentListener")
    """

    short_countries = ["IT", "IT-CNO", "IT-CSO", "IT-NO", "IT-SAR", "IT-SIC", "IT-SO", "SE", "SE-SE1", "SE-SE2", "SE-SE3", "SE-SE4"]
    short_years = ["2021", "2022", "2023", "2024"]
    
    long_countries = [
        # Paesi europei
        "AT", "BE", "FR", "FI", "DE", "GB", "IE", "IT", "NO", "PL",
        "CZ", "SI", "ES", "SE", "CH",  
    ]
    long_years = ["2024"]

    # Paesi con tutti gli anni
    for country in short_countries:
        for year in short_years:
            url = f"https://data.electricitymaps.com/2025-04-03/{country}_{year}_{granularity}.csv"
            send_url_to_nifi(url, nifi_endpoint)
    
    # Paesi solo con il 2024
    for country in long_countries:
        for year in long_years:
            url = f"https://data.electricitymaps.com/2025-04-03/{country}_{year}_yearly.csv"
            send_url_to_nifi(url, nifi_endpoint)


# # Paesi extra-europei
#             "US", "AE", "CN", "IN", "JP", "BR", "MX", "CA", "AU", "ZA",
#             "KR", "ID", "SG", "AR", "EG"