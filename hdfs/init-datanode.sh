#!/bin/bash
chown -R hadoop:hadoop /opt/hadoop/data/dataNode
chmod -R 777 /opt/hadoop/data/dataNode
hdfs datanode
