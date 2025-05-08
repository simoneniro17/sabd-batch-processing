# sabd-batch-processing

--- NOTE GENERALI ---

- Da base oraria aggreghiamo su base annuale con query 1
- Graphana come tool di visualizzazione dei dati
- Usare sempre spark per fare la classifica (no sorting manuale).
- Come fare in maniera efficiente il calcolo dei percentili. 
- Possiamo usare sia dati di spark che i dati interni per la misurazione delle prestazioni e le tempistiche (e.g. tempo di processamento).
- Mai una sola esecuzione, ma di più per avere dei dati affidabili dal punto di vista statistico (e.g. valore + dev.std).
- Docker Compose per orchestrare i container in esecuzione sulla stessa macchina.
- BONUS: Si possono variare un po’ di parametri di configurazione di Spark (e.g. numero di executor, numero di worker node ecc.) per vedere se ci sono miglioramenti.
- NiFi per prendere e convertire i dati in parquet (?)


1. Installare estensione Docker per VSCode (se non già installata)
2. Installare Docker Desktop (se non già installato)
3. Una volta avviato Docker Desktop, aprire il terminale di VSCode e digitare il comando `docker-compose up -d` per avviare i container (per conferma lanciare `docker ps` per vedere i container in esecuzione). L'immagine di Spark è già presente nel file `docker-compose.yml` e verrà scaricata automaticamente se non è già presente.
4. Dopodichè lanciare `docker exec -it sabd-batch-processing-spark-client-1 bash` per entrare nel container di Spark e lanciare i comandi di Spark. Per uscire dal container digitare `exit`.
Poi eseguire il comando `spark-submit /app/try.py --master spark://spark-master:7077`

FORSE VA INSTALLATO JAVA