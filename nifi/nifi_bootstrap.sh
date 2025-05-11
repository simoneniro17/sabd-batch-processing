#!/bin/bash

# Avvia NiFi in background
/opt/nifi/nifi-current/bin/nifi.sh start

# Aspetta che NiFi sia pronto (opzionale ma consigliato)
sleep 20

# Esegui lo script una sola volta
python3 /app/nifi_url_feeder.py --mode LONG

# Tieni vivo il container
tail -f /opt/nifi/nifi-current/logs/nifi-app.log
