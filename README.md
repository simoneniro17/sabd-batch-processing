# Obiettivo Generale del Progetto
L'obiettivo principale è usare il framework di data processing Apache Spark per rispondere ad alcune query su dati storici forniti da Electricity Maps sull’elettricià e sulle emissioni di CO2 per produrla per **comprendere quanto sia pulita o sostenibile la rete energetica**.


# Attività Richieste
Le attività sono:
*   Svolgere le Query 1, 2 e 3.
*   Svolgere le attività di Acquisizione e Ingestione Dati.
*   Svolgere la parte opzionale A (Query 4).

La composizione del team deve essere inviata via email entro il 23 Maggio.


# Dataset
Electricity Maps fornisce un dataset per ogni paese e per diverse granularità temporali (ora, giorno, mese, anno).
Il dataset di ogni paese contiene 35065 eventi, che descrivono la produzione di elettricità dal 1 gennaio 2021 al 31 dicembre 2024.

## Dati Rilevanti da Analizzare
Ogni evento è caratterizzato da:
*   **Intensità di carbonio**: quantità di gas serra emessi per unità di elettricità, misurata in grammi di CO₂ per kilowatt-ora (gCO₂/kWh). Sono forniti sia i fattori di emissione diretti che quelli relativi al ciclo di vita.
*   **Percentuale di energia a zero emissioni (Carbon-Free Energy share - CFE)**: percentuale di elettricità disponibile sulla rete da fonti a basse o nulle emissioni di CO2. Sono esclusi i combustibili fossili e sono inclusi l'energia solare, eolica, idroelettrica, geotermica, da biomassa e nucleare.
*   **Percentuale di energia rinnovabile**: percentuale di elettricità disponibile sulla rete da fonti rinnovabili. Sono inclusi l'energia da biomassa, geotermica, idroelettrica, solare ed eolica.
 
## Focus del Progetto
Per questo progetto, il focus principale sarà sull'**Italia e la Svezia** con granularità oraria, per gli anni dal 2021 al 2024. L'Italia è suddivisa in 6 macro-regioni e la Svezia in 4.
Durante la fase di data ingestion, valutare il modo più opportuno di memorizzare i dati: se tramite file separati o tramite un unico file per il periodo dal 2021 al 2024.
Inoltre, per gli scopi del progetto, sono di interesse solo i dati relativi all'**intensità di carbonio** e la **precentuale di energia senza emissioni di carbonio**.
Il dataset dovrà essere scaricato durante la fase di ingestione dei dati.


# Elaborazione Dati (Spark)
Per l'elaborazione dei dati e per rispondere alle query, è necessario utilizzare il **framework Spark**.
È consentito l'utilizzo di RDDs o DataFrames, ma **senza l'uso di SQL** (a meno che non si svolga l'Optional Part B).
Il linguaggio di programmazione scelto è Python.
I risultati delle query devono essere salvati in formato **CSV**.
È richiesto di **includere nel report e nelle slide i tempi di risposta delle query** sulla piattaforma di riferimento utilizzata.


# Query Richieste
I risultati di ciascuna query deve essere consegnato in formato CSV. Per la rappresentazione grafica dei risultati delle query, utilizzare un framework di visualizzazione (e.g. Grafana).
Considerando il dataset indicato, le query a cui rispondere sono le seguenti.
## Query 1 (Italia e Svezia)
*   Aggregare i dati su base annua (yearly basis). Calcolare la media (average), il minimo (minimum) e il massimo (maximum) dell'**intensità di carbonio** (diretta) e della **percentuale di energia a zero emissioni (CFE%)** per ciascun anno dal 2021 al 2024.
*   Utilizzando **solo i valori medi** (average) delle due metriche (intensità di carbonio e CFE%) aggregati su base annua, generare **due grafici** per confrontare visivamente l'andamento (trend) per Italia e Svezia.
### Esempio di output
```
Esempio di output:
# date, country, carbon-mean, carbon-min, carbon-max, cfe-mean, cfe-min, cfe-max
2021, IT, 280.08, 121.24, 439.06, 46.305932, 15.41, 77.02
2022, IT, 321.617976, 121.38, 447.33, 41.244127, 13.93, 77.44
...
2021, SE, 5.946325, 1.50, 55.07, 98.962411, 92.80, 99.65
2022, SE, 3.875823, 0.54, 50.58, 99.551723, 94.16, 99.97
```

## Query 2 (Solo Italia)
*   Aggregare i dati sulla coppia (anno, mese) calcolando il valor medio dell'**intensità di carbonio** e della **percentuale di energia a zero emissioni (CFE%)**.
*   Calcolare la classifica (sempre usando Spark, no sorting manuale) delle **prime 5 coppie (anno, mese)** con:
    *   Intensità di carbonio più alta (highest).
    *   Intensità di carbonio più bassa (lowest).
    *   Quota di energia a zero emissioni (CFE) più bassa (lowest).
    *   Quota di energia a zero emissioni (CFE) più alta (highest).
    In totale sono attesi 20 valori.
*   Considerando il valor medio delle due metriche aggregati sulla coppia (anno, mese), generare **due grafici** per valutare visivamente  l'andamento (trend) delle due metriche.
### Esempio di output
```
# date, carbon-intensity, cfe
2022 12, 360.520000, 35.838320
2022 3, 347.359073, 35.822218
2021 11, 346.728514, 33.076681
2022 10, 335.784745, 39.167164
2022 2, 330.489896, 38.980595

2024 5, 158.240887, 68.989731
2024 4, 170.670889, 66.253958
2024 6, 171.978792, 65.487792
2024 3, 192.853871, 60.919556
2024 7, 200.595995, 57.939099

2024 5, 158.240887, 68.989731
2024 4, 170.670889, 66.253958
2024 6, 171.978792, 65.487792
2024 3, 192.853871, 60.919556
2023 5, 203.494489, 59.877003

2021 11, 346.728514, 33.076681
2022 3, 347.359073, 35.822218
2022 12, 360.520000, 35.838320
2022 1, 326.947876, 36.603683
2021 12, 329.303508, 37.868817
```

## Query 3 (Italia e Svezia)
*   Aggregare i dati di ciascun paese su un **periodo di 24 ore**.
*   Calcolare il valore medio (average) dell'**intensità di carbonio** e della **percentuale di energia a zero emissioni (CFE%)** per ogni periodo di 24 ore.
*   Per questi valori medi calcolati sulle 24 ore, computare il minimo (minimum), il 25° percentile, il 50° percentile (mediana), il 75° percentile e il massimo (maximum) per ciascuna delle due metriche (intensità di carbonio e CFE%).
*   Considerando sempre il valor medio delle due metriche aggregati sulle 24 fasce orarie giornaliere, generare **due grafici** per confrontare visivamente l'andamento (trend).
### Esempio di output
```
# county, data, min, 25-perc, 50-perc, 75-perc, max
IT, carbon-intensity, 219.029329, 241.060318, 279.202916, 285.008504, 296.746208
IT, cfe, 42.203176, 45.728436, 47.600110, 53.149180, 57.423648
SE, carbon-intensity, 3.150062, 3.765761, 4.293638, 4.876138, 5.947180
SE, cfe, 99.213936, 99.338007, 99.411328, 99.472495, 99.540979
```

## Optional Part A (Obbligatoria) - Query 4
*   Eseguire un'analisi di clustering sui dati relativi all'**intensità di carbonio**, aggregati su base annua e **per l'anno 2024**. I dati fanno riferimento ai valori medi annui per ciascun paese.
*   L'obiettivo è individuare l'insieme di nazioni con comportamenti simili in termini di emissioni di carbonio, usando l'**algoritmo di clustering K-means**.
*   Oltre ad applicare l'algoritmo di clustering sui dati, determinare un valore ottimale di k (numero di cluster) utilizzando il **metodo del gomito** (elbow method) o l'**indice di silhouette** (silhouette index).
*   Si considerino i seguenti 15 paesi europei: Austria, Belgio, Francia, Finlandia, Germania, Gran Bretagna, Irlanda, Italia, Norvegia, Polonia, Repubblica Ceca, Slovenia, Spagna, Svezia e Svizzera. Si scelgano inoltre 15 paesi extra-europei, tra cui Stati Uniti, Emirati Arabi, Cina, India, per un totale di 30 paesi a livello mondiale.
*   Generare un grafico che consenta di visualizzare il risultato del clustering.


## Optional Part B (Facoltativa)
*   Utilizzare **Spark SQL** per eseguire le query usando SQL.
*   Comparare le prestazioni con quelle ottenute utilizzando RDDs o DataFrames, riportando l'analisi del confronto nella relaizone e nella presentazione.


# Acquisizione e Ingestione Dati
Si chiede di realizzare la fase di data ingestion per:
*   importare i dati di input in HDFS, eventualmente gestendo la conversione del formato dei dati, usando un framework di data ingestion a scelta. Noi utilizzeremo **Apache NiFi**.
*   esportare i risultati di output da HDFS ad un sistema di storage a scelta. Noi utilizzeremo **Redis**.
È necessario scegliere un formato per archiviare ed elaborare i dati. Esempi di formati menzionati sono csv, formati colonnari (Parquet, ORC), formati a riga (Avro), ecc.. Noi scegieremo il formato CSV.
È necessario decidere dove esportare i risultati delle query. Esempi di destinazioni menzionate sono HBase, Redis, Kafka, ecc..


# Piattaforma e Valutazione Prestazioni
È richiesto di **valutare sperimentalmente i tempi di elaborazione delle query** sulla piattaforma di riferimento scelta per la realizzazione del progetto e di riportare tali tempi nella relazione e nella presentazione del progetto.
Tale piattaforma può essere un **nodo standalone** (si suggerisce di usare **Docker Compose** per orchestrare più container sulla stessa macchina).
Per la misurazione delle prestazioni e dei tempi è possibile usare sia dati di Spark che dati interni.
Per avere dati più affidabili dal punto di vista statistico, non bisogna mai fare una sola esecuzione. Inoltre, bisogna accompagnare i risultati con informazioni come la deviazione standard per dargli maggiore credibilità.


# Consegne e Scadenze
Le consegne richieste e le relative scadenze sono:
*   **Invio composizione team via email**: May 23.
*   **Scadenza consegna progetto**: June 9, 2025.
*   **Presentazione**: June 16, 2025 (data da confermare). La presentazione durerà al massimo 15 minuti per team.

I documenti e materiali da consegnare sono:
*   Link a storage cloud o repository contenente il codice del progetto. Inserire i risultati delle query in formato CSV in una cartella denominata `Results`.
*   **Report di progetto**: composto da 4-8 pagine, in formato ACM o IEEE proceedings. Il report deve includere i tempi di risposta delle query sulla piattaforma utilizzata. Includere nella relazione lo schema dell'architettura di sistema utilizzata.
*   Slide di presentazione: da consegnare dopo la presentazione.


# Note Generali Utili
- Consigliabile l'utilizzo di Graphana come tool di visualizzazione dei dati per dashboard. 
- BONUS: Variare i parametri di configurazione di Spark (e.g. numero di executor, numero di worker node) per vedere se ci sono miglioramenti.


# Workflow di Prova per Spark
1. Avviare Docker Desktop.
2. Aprire il terminale di VSCode e digitare il comando `docker-compose up -d` per avviare i container (per conferma lanciare `docker ps` per vedere i container in esecuzione). L'immagine di Spark è già presente nel file `docker-compose.yml` e verrà scaricata automaticamente se non è già presente.
3. Lanciare `docker exec -it sabd-batch-processing-spark-client-1 bash` per entrare nel container di Spark. Per uscire dal container digitare `exit`.
4. Eseguire `spark-submit /app/try.py --master spark://spark-master:7077`. Se dovesse dare problemi, lanciare `find / -name spark-submit 2>/dev/null` per trovare il percorso corretto del comando `spark-submit` e usarlo al posto di quello di default. In questo caso il comando diventa `/opt/spark/bin/spark-submit /app/try.py --master spark://spark-master:7077`.
