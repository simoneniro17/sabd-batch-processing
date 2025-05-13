#!/bin/bash

# Directory che vogliamo resettare
datanode_dir1="./datanode1"
datanode_dir2="./datanode2"
namenode_dir="./namenode"

# Funzione per cambiare i permessi
change_permissions() {
  dir=$1
  echo "Modificando permessi per $dir..."
  sudo chmod -R 777 $dir
  sudo chown -R $USER:$USER $dir
}

# Funzione per rimuovere il contenuto di una directory
remove_content() {
  dir=$1
  echo "Rimuovendo contenuto di $dir..."
  sudo rm -rf $dir/*
}

# Cambia i permessi e rimuovi i contenuti delle directory
change_permissions $datanode_dir1
change_permissions $datanode_dir2
change_permissions $namenode_dir

remove_content $datanode_dir1
remove_content $datanode_dir2
remove_content $namenode_dir

echo "Resettato il contenuto di tutte le directory Hadoop."
