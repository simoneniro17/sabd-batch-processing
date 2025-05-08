# REQUISITI PER IL CORRETTO FUNZIONAMENTO DEL PROGETTO
- Docker e Docker Compose
- Java (preferibilmente 8 o 11, perché supportate meglio da Spark) con JAVA_HOME configurato
- Python 3.8+
- (OPZIONALE) Estensione Docker per VSCode (una volta installata si dovrebbe chiamare Containers Tool)


# WORFKLOW DI PROVA
1. Avviare Docker Desktop.
2. Aprire il terminale di VSCode e digitare il comando `docker-compose up -d` per avviare i container (per conferma lanciare `docker ps` per vedere i container in esecuzione). L'immagine di Spark è già presente nel file `docker-compose.yml` e verrà scaricata automaticamente se non è già presente.
3. Lanciare `docker exec -it sabd-batch-processing-spark-client-1 bash` per entrare nel container di Spark. Per uscire dal container digitare `exit`.
4. Eseguire `spark-submit /app/try.py --master spark://spark-master:7077`. Se dovesse dare problemi, lanciare `find / -name spark-submit 2>/dev/null` per trovare il percorso corretto del comando `spark-submit` e usarlo al posto di quello di default. In questo caso il comando diventa `/opt/spark/bin/spark-submit /app/try.py --master spark://spark-master:7077`.

In alternativa, se si è installato pyspark in locale, dovrebbe essere possibile eseguire il codice attualmente commentato dello script `try.py`.


# NOTE GENERALI (appunti presi durante la lezione)

- Da base oraria aggreghiamo su base annuale con query 1
- Graphana come tool di visualizzazione dei dati
- Usare sempre spark per fare la classifica (no sorting manuale).
- Come fare in maniera efficiente il calcolo dei percentili. 
- Possiamo usare sia dati di spark che i dati interni per la misurazione delle prestazioni e le tempistiche (e.g. tempo di processamento).
- Mai una sola esecuzione, ma di più per avere dei dati affidabili dal punto di vista statistico (e.g. valore + dev.std).
- Docker Compose per orchestrare i container in esecuzione sulla stessa macchina.
- BONUS: Si possono variare un po’ di parametri di configurazione di Spark (e.g. numero di executor, numero di worker node ecc.) per vedere se ci sono miglioramenti.
- NiFi per prendere e convertire i dati in parquet (?)