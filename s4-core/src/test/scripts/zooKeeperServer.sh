#!/bin/sh

CLASSPATH=$1
PORT=$2
DATA_DIR=$3

echo java -cp $CLASSPATH org.apache.zookeeper.server.ZooKeeperServerMain $PORT $DATA_DIR

java -cp $CLASSPATH org.apache.zookeeper.server.ZooKeeperServerMain $PORT $DATA_DIR &
