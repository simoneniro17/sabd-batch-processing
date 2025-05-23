# Analisi Energetica con Apache Spark - Progetto SABD (Batch Processing)

## Panoramica del Progetto

In questo progetto abbiamo utilizzato **Apache Spark** per analizzare dati storici sull'elettricità e sulle emissioni di CO₂ (forniti da **Electricity Maps**), con l'obiettivo di capire quanto sia pulita e sostenibile la rete elettrica in diversi paesi.

### Obiettivi
Gli obiettivi richiesti dalla traccia erano:
- Analizzare l'intensità di carbonio e la percentuale di energia a zero emissioni
- Confrontare le performance energetiche di Italia e Svezia (2021-2024)
- Implementare clustering per identificare pattern simili tra paesi europei
- Valutare le prestazioni di elaborazione su piattaforma Spark
Inoltre, abbiamo scelto di svolgere anche la parte opzionale relativa all'implementazione delle query in **Spark SQL**.

## Dataset

I dati di **Electricity Maps** che sono stati utilizzati forniscono diverse informazioni, ma quelle su cui ci siamo concentrati sono:

- **Intensità di carbonio (diretta)**: grammi di CO₂ per kilowatt-ora (gCO₂/kWh)
- **Percentuale di energia a zero emissioni (CFE)**: quota di elettricità da fonti a basse/nulle emissioni

### Scope
- **Paesi su cui ci siamo concentrati**: Italia e Svezia
- **Periodo**: 2021-2024
- **Granularità**: Dati orari
- **Volume**: c.a. 35k eventi per paese

## Query Implementate

### Query 1: Analisi Annuale (Italia e Svezia)
Aggregazione su base annua con calcolo di media, minimo e massimo per intensità di carbonio e CFE%.

### Query 2: Ranking Mensile (Solo Italia)
Classifica delle prime 5 coppie (anno, mese) per:
- Intensità di carbonio più alta/bassa
- CFE% più alta/bassa

### Query 3: Analisi Distributiva 24h (Italia e Svezia)
Calcolo di percentili (25°, 50°, 75°), massimo e minimo per valori medi giornalieri.

### Query 4: Clustering K-means
Analisi di clustering su 30 paesi (15 europei + 15 extra-europei) per identificare pattern simili nelle emissioni di carbonio del 2024.


## Avvio e Utilizzo

### Installazione

1. **Clonare il repository**
```bash
git clone <repository-url>
cd sabd-batch-processing
```

2. **Installare le dipendenze Python**
```bash
pip install -r requirements.txt
```

### CLI Interattiva

Per (cercare di) semplificare la gestione dell'intero workflow, abbiamo creato una CLI interattiva (`main_cli.py`) con l'aiuto dell'IA.

```bash
python main_cli.py
```

### Esecuzione da Riga di Comando

In alternativa, è anche possibile utilizzare argomenti da linea di comando ed aggirare la CLI.

```bash
# Eseguire una query specifica con modalità DataFrame e 3 esecuzioni
python main_cli.py --query 1 --mode dataframe --runs 3

# Esegui una query specifica con Spark SQL
python main_cli.py --query 2 --mode sql

# Mostra tutte le opzioni disponibili
python main_cli.py --help
```

## Architettura

```
├── Data Ingestion e Preprocessing (con Apache NiFi)
│   ├── Download automatico dei dataset
│   ├── Pulizia e conversione da CSV a Parquet
│   └── Caricamento finale in HDFS
├── Distributed File System (HDFS)
│   ├── Dati da NiFi
│   ├── Risultati delle query Spark
│   ├── Risultati delle valutazioni delle prestazioni
├── Processing (Apache Spark)
│   ├── DataFrame API per le query 1-3
│   └── MLlib (K-means clustering) per la query 4
│   ├── Spark SQL per le query 1-3
├── Data Storage (Redis)
│   ├── Carica i dati da HDFS in Redis
│   ├── Scarica i dati da Redis in locale
└── Data Visualization (Grafana)
```

### Ambiente di Deployment

L'infrastruttura è stata orchestrata tramite Docker Compose e include:

- **HDFS Cluster**: 1 NameNode + 2 DataNode
- **Spark Cluster**: 1 Master + 1 Worker
- **Apache NiFi**: Singola istanza per data ingestion
- **Redis**: Server singolo per caching e storage risultati
- **Grafana**: Dashboard per visualizzazione con plugin CSV

L'intera configurazione utilizza una rete Docker dedicata (`172.20.0.0/16`) per la comunicazione tra i componenti.


## Data Pipeline

### Acquisizione, Ingestione Dati e Preprocessamento
La fase di data ingestion e preprocessing comprende:
- Ricezione degli URL dei dataset da scaricare da uno script Python, che li invia a una porta su cui **Apache NiFi** è in ascolto
- Pulizia (sono state scartate le colonne che non sarebbero state utili per le query implementate)
- Conversione in formato Parquet a seconda della granularità (solo per i dataset con granularità oraria)
- Importazione dei dati in HDFS

### Elaborazione
- Utilizzo di **Apache Spark** con DataFrame API e Spark SQL
- Salvataggio in HDFS dei risultati delle query in formato CSV, in una cartella dedicata a ciascuna query
- Salvataggio in HDFS delle valutazioni delle prestazioni in formato CSV, in una cartella dedicata a ciascuna query

### Archiviazione
- Utilizzo di **Redis** per il caricamento dei dati da HDFS e lo scaricamento dei risultati in locale

### Visualizzazione
- Utilizzo di **Grafana** per la visualizzazione dei risultati delle query e delle valutazioni delle prestazioni