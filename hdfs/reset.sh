#!/bin/bash

# Directory da resettare
datanode_dir1="./datanode1"
datanode_dir2="./datanode2"
namenode_dir="./namenode"


change_permissions() {
  dir=$1
  echo "Modificando permessi per $dir..."
  sudo chmod -R 777 $dir
  sudo chown -R $USER:$USER $dir
}


remove_content() {
  dir=$1
  echo "Rimuovendo contenuto di $dir..."
  sudo rm -rf $dir/*
}


change_permissions $datanode_dir1
change_permissions $datanode_dir2
change_permissions $namenode_dir

remove_content $datanode_dir1
remove_content $datanode_dir2
remove_content $namenode_dir

echo "Resettato il contenuto di tutte le directory Hadoop."
