# Obiettivo Generale del Progetto
L'obiettivo principale è analizzare un dataset per **comprendere quanto sia pulita o sostenibile la rete energetica**.


# Attività Richieste
Le attività sono:
*   Svolgere le Query 1, 2 e 3.
*   Svolgere le attività di Acquisizione e Ingestione Dati.
*   Svolgere la parte opzionale A (Query 4).

La composizione del team deve essere inviata via email entro il 23 Maggio.


# Dataset
Il dataset riguarda le **emissioni di CO₂ dell'elettricità**. È un dataset di dati reali e in tempo reale sulla produzione di elettricità, fornito da Electricity Maps.
Il dataset è disponibile per ogni paese e per diverse granularità temporali (ora, giorno, mese, anno).

## Focus del Progetto
Per questo progetto, il focus principale sarà sull'**Italia e la Svezia**, considerando una **granularità oraria** dal 1 Gennaio 2021 al 31 Dicembre 2024.
Il dataset per ogni paese contiene 35065 eventi. L'Italia è suddivisa in 6 macro-regioni e la Svezia in 4.
Il dataset dovrà essere scaricato durante la fase di ingestione dei dati.

## Dati Rilevanti da Analizzare
I dati rilevanti da analizzare includono:
*   **Intensità di carbonio dell'elettricità consumata**: quantità di anidride carbonica (CO₂) emessa per unità di elettricità consumata, misurata in grammi di CO₂ per kilowatt-ora (gCO₂/kWh). Varia in base al mix di fonti energetiche.
*   **Quota di energia a zero emissioni (Carbon-Free Energy share - CFE)**: percentuale di elettricità proveniente da fonti rinnovabili e a basso contenuto di carbonio (incluse biomasse, geotermico, idroelettrico, solare, eolico, **nonché il nucleare**).
*   Quota di energia rinnovabile: percentuale di elettricità proveniente solo da fonti rinnovabili.


# Elaborazione Dati (Spark)
Per l'elaborazione dei dati e per rispondere alle query, è necessario utilizzare il **framework Spark**.
È consentito l'utilizzo di RDDs o DataFrames, ma **senza l'uso di SQL** (a meno che non si svolga l'Optional Part B).
Il linguaggio di programmazione scelto è Python.
I risultati delle query devono essere salvati in formato **CSV**.
È richiesto di **includere nel report e nelle slide i tempi di risposta delle query** sulla piattaforma di riferimento utilizzata.


# Query Richieste
## Query 1 (Italia e Svezia)
*   Calcolare la media (average), il minimo (minimum) e il massimo (maximum) dell'**intensità di carbonio** e della **quota di energia a zero emissioni (CFE)** dal 2021 al 2024 su base annuale (yearly basis). Da base oraria occorre quindi aggregare su base annuale.
*   Utilizzando **solo i valori medi** (average) delle due metriche (intensità di carbonio e CFE), generare **due grafici** per confrontare visivamente l'andamento (trend) per l'Italia e la Svezia.

## Query 2 (Solo Italia)
*   Calcolare la media mensile (monthly average) dell'**intensità di carbonio** e della **quota di energia a zero emissioni (CFE)**.
*   Calcolare la classifica (sempre usando Spark, no sorting manuale) dei **5 migliori e 5 peggiori coppie (anno, mese)** con:
    *   Intensità di carbonio più alta (highest).
    *   Intensità di carbonio più bassa (lowest).
    *   Quota di energia a zero emissioni (CFE) più bassa (lowest).
    *   Quota di energia a zero emissioni (CFE) più alta (highest).
*   Generare **due grafici** per visualizzare l'andamento (trend) delle due metriche (intensità di carbonio e CFE) nel tempo.


## Query 3 (Italia e Svezia)
*   Aggregare i dati su un **periodo di 24 ore**.
*   Calcolare il valore medio (average) dell'**intensità di carbonio** e della **quota di energia a zero emissioni (CFE)** per ogni periodo di 24 ore.
*   Per questi valori medi calcolati sulle 24 ore, computare il minimo (minimum), il 25° percentile, il 50° percentile (mediana), il 75° percentile e il massimo (maximum).
*   Generare **due grafici** per visualizzare l'andamento (trend) dei **valori medi** (average values) aggregati sulle 24 ore.

## Optional Part A (Obbligatoria) - Query 4
*   Utilizzare l'algoritmo di clustering **K-means**.
*   Applicare K-means su un insieme selezionato di paesi.
*   L'obiettivo è raggruppare i paesi in base all'**intensità di carbonio calcolata su base annuale (yearly basis), specificamente per l'anno 2024**.
*   Trovare il numero di cluster ottimale.

## Optional Part B (Facoltativa)
*   Utilizzare **Spark SQL** per eseguire le query e comparare le prestazioni con quelle ottenute utilizzando RDDs o DataFrames.


# Acquisizione e Ingestione Dati
Il dataset deve essere scaricato durante la fase di ingestione.
È necessario scegliere un framework per l'ingestione dei dati in HDFS. Noi utilizzeremo il framework NiFi.
È obbligatorio utilizzare **HDFS** per il download del dataset.
È necessario scegliere un formato per archiviare ed elaborare i dati. Esempi di formati menzionati sono csv, formati colonnari (Parquet, ORC), formati a riga (Avro), ecc.. Noi scegieremo il formato CSV.
È necessario decidere dove esportare i risultati delle query. Esempi di destinazioni menzionate sono HBase, Redis, Kafka, ecc..


# Piattaforma e Valutazione Prestazioni
È richiesto di **valutare sperimentalmente i tempi di elaborazione delle query** sulla piattaforma di riferimento scelta.
La piattaforma può essere un **nodo standalone**.
È raccomandato l'uso di **Docker Compose** per orchestrare più container sulla stessa macchina.
Per la misurazione delle prestazioni e dei tempi è possibile usare sia dati di Spark che dati interni.
Per avere dati più affidabili dal punto di vista statistico, non bisogna mai fare una sola esecuzione. Inoltre, bisogna accompagnare i risultati con informazioni come la deviazione standard per dargli maggiore credibilità.


# Consegne e Scadenze
Le consegne richieste e le relative scadenze sono:
*   **Invio composizione team via email**: May 23.
*   **Scadenza consegna progetto**: June 9, 2025.
*   **Presentazione**: June 16, 2025 (data da confermare). La presentazione durerà al massimo 15 minuti per team.

I documenti e materiali da consegnare sono:
*   Link a storage cloud o repository contenente il codice del progetto.
*   **Report di progetto**: composto da 4-8 pagine, in formato ACM o IEEE proceedings. Il report deve includere i tempi di risposta delle query sulla piattaforma utilizzata.
*   Slide di presentazione: da consegnare dopo la presentazione.


# Note Generali Utili
- Consigliabile l'utilizzo di Graphana come tool di visualizzazione dei dati per dashboard. 
- BONUS: Variare i parametri di configurazione di Spark (e.g. numero di executor, numero di worker node) per vedere se ci sono miglioramenti.


# Workflow di Prova per Spark
1. Avviare Docker Desktop.
2. Aprire il terminale di VSCode e digitare il comando `docker-compose up -d` per avviare i container (per conferma lanciare `docker ps` per vedere i container in esecuzione). L'immagine di Spark è già presente nel file `docker-compose.yml` e verrà scaricata automaticamente se non è già presente.
3. Lanciare `docker exec -it sabd-batch-processing-spark-client-1 bash` per entrare nel container di Spark. Per uscire dal container digitare `exit`.
4. Eseguire `spark-submit /app/try.py --master spark://spark-master:7077`. Se dovesse dare problemi, lanciare `find / -name spark-submit 2>/dev/null` per trovare il percorso corretto del comando `spark-submit` e usarlo al posto di quello di default. In questo caso il comando diventa `/opt/spark/bin/spark-submit /app/try.py --master spark://spark-master:7077`.
