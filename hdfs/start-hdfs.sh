#!/bin/bash

# Format del NameNode
if [ ! -d "/opt/hadoop/data/nameNode/current" ]; then
    echo "Formattando NameNode..."
    hdfs namenode -format
fi

# Avvio del NameNode
echo "Inizializzando NameNode..."
hdfs namenode &

# Salva il PID 
NAMENODE_PID=$!

# Controllare se HDFS è pronto
until hdfs dfs -ls / &> /dev/null; do
    echo "Waiting for HDFS to be ready..."
    sleep 2
done

sleep 60

# Crea la directory data e imposta i permessi
echo "Creazione directory /data in HDFS..."
hdfs dfs -test -e /data || hdfs dfs -mkdir /data
hdfs dfs -chmod -R 777 /data

echo "Creazione directory /results in HDFS..."
hdfs dfs -test -e /results || hdfs dfs -mkdir /results
hdfs dfs -chmod -R 777 /results

# Attende il processo del NameNode (così il container resta attivo)
wait $NAMENODE_PID
