#!/bin/bash

# Controllo parametro
if [ "$#" -ne 1 ]; then
  echo "Uso: $0 <NOME_FILE>"
  echo "   Es: $0 SE_2022_hourly.csv"
  exit 1
fi

# Parametri
FILENAME="$1"
HDFS_PATH="/data/$FILENAME"
TMP_PATH="/tmp/$FILENAME"
LOCAL_PATH="./$FILENAME"
CONTAINER_NAME="namenode"

echo "Scarico da HDFS: $HDFS_PATH ..."
docker exec "$CONTAINER_NAME" hdfs dfs -get -f "$HDFS_PATH" "$TMP_PATH"

if [ $? -ne 0 ]; then
  echo "Errore durante il download da HDFS"
  exit 1
fi

echo "Copio dal container ($TMP_PATH) a $LOCAL_PATH ..."
docker cp "$CONTAINER_NAME":"$TMP_PATH" "$LOCAL_PATH"

if [ $? -ne 0 ]; then
  echo "Errore durante la copia dal container"
  exit 1
fi

echo "Fatto! File salvato in: $LOCAL_PATH"
