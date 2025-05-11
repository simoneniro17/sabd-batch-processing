docker exec -it namenode bash -c "
hdfs dfs -test -e /data || hdfs dfs -mkdir /data && \
hdfs dfs -chmod -R 777 /data && \
hdfs dfs -ls /data
"
# crea la cartella data e da a nifi i permessi