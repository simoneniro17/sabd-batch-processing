import requests
import time

# Verifica se la cartella "/data" esiste in HDFS (e quindi anche se il NameNode è fuori dalla Safemode) 
def is_namenode_ready(namenode_host = "localhost", port = 9870):
    url = f"http://{namenode_host}:{port}/webhdfs/v1/data?op=GETFILESTATUS"
    try:
        response = requests.get(url)
        if response.status_code == 200:
            return True
        elif response.status_code == 404:
            return False
        else:
            print(f"Errore durante la verifica della cartella /data in HDFS: {response.status_code}")
            return False
    except Exception as e:
        print(f"Eccezione durante la verifica della cartella /data in HDFS: {e}")
        return False


# Funzione per inviare URL a NiFi
def send_url_to_nifi(url, endpoint):
    response = requests.post(endpoint, data=url)
    
    if response.status_code == 200:
        print(f"Inviato con successo URL: {url}")
    else:
        print(f"Errore nell'invio di {url}\nStatus: {response.status_code}")
        if hasattr(response, 'text'):
            print(f"Risposta errore: {response.text[:100]}")


def feed_nifi_urls(granularity="hourly", nifi_endpoint="http://localhost:1406/contentListener"):
    """
    Genera gli URL dei dataset di Electricity Maps da inviare a NiFi e li invia tramite `send_url_to_nifi`.

    Args:
        granularity (str): Livello di granularità del dataset ['hourly', 'daily', 'monthly', 'yearly']. Default è "hourly".
        nifi_endpoint (str): Endpoint HTTP di NiFi a cui inviare gli URL. Default è "http://localhost:1406/contentListener".
    """

    while not is_namenode_ready():
        print("Cartella /data non disponibile in HDFS, attendo...")
        time.sleep(5)

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
