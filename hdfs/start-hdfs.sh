#!/bin/bash

# Format del NameNode solo se necessario
if [ ! -d "/opt/hadoop/data/nameNode/current" ]; then
    echo "Formatting NameNode..."
    hdfs namenode -format
fi

# Avvio del NameNode in background
echo "Starting NameNode..."
hdfs namenode &

# Salva il PID per eventuali cleanup
NAMENODE_PID=$!

# Funzione per controllare se HDFS è pronto
until hdfs dfs -ls / &> /dev/null; do
    echo "Waiting for HDFS to be ready..."
    sleep 2
done

sleep 60

# Crea la directory data e imposta i permessi per permettere la scrittura
echo "Setting up /data directory in HDFS..."
hdfs dfs -test -e /data || hdfs dfs -mkdir /data
hdfs dfs -chmod -R 777 /data

echo "Setting up /results directory in HDFS..."
hdfs dfs -test -e /results || hdfs dfs -mkdir /results
hdfs dfs -chmod -R 777 /results

# Attende il processo del NameNode (così il container resta attivo)
wait $NAMENODE_PID
