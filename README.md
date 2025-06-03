# Analisi Energetica con Apache Spark - Progetto SABD (Batch Processing)

## Panoramica del Progetto

In questo progetto abbiamo utilizzato **Apache Spark** per analizzare dati storici sull'elettricità e sulle emissioni di CO₂ (forniti da **Electricity Maps**), con l'obiettivo di capire quanto sia pulita e sostenibile la rete elettrica in diversi paesi.

## Avvio e Utilizzo

### Installazione

Per eseguire il progetto, è necessario avere installato **Python 3.8+** e **Docker**. Dopodiché, seguire i passaggi seguenti:

**Clonare il repository**

```bash
git clone https://github.com/simoneniro17/sabd-batch-processing
cd sabd-batch-processing
```

**Installare le dipendenze**
```bash
pip install -r requirements.txt
```

### CLI Interattiva

Per (cercare di) semplificare la gestione dell'intero workflow, abbiamo creato una CLI interattiva (`main_cli.py`) con il supporto dell'IA.
Per avviare la CLI è sufficiente eseguire il seguente comando:

```bash
python main_cli.py
```

### Esecuzione da Riga di Comando

In alternativa, è anche possibile utilizzare direttamente alcuni argomenti da linea di comando ed aggirare la CLI.

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
├── Data Acquisition, Preprocessing & Ingestion (con Apache NiFi)
│   ├── Download dei dataset di Electricity Maps
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
│   ├── Visualizzazione dei risultati delle query salvati in locale
```

### Ambiente di Deployment

L'infrastruttura è stata orchestrata tramite Docker Compose e i cluster composti da più di un container sono:

- **HDFS Cluster**: 1 NameNode + 2 DataNode
- **Spark Cluster**: 1 Master + 2 Worker

L'intera configurazione utilizza una rete Docker dedicata (`172.20.0.0/16`) per la comunicazione tra i componenti.
