#!/bin/bash

TOPIC=$1
KAFKA_HOME=/opt/kafka
#BROKER=10.158.9.102:9092
BROKER=10.158.9.201:9092
LOG_DIR=/path/to/kafka-logs

# Stop Kafka
$KAFKA_HOME/bin/kafka-server-stop.sh

# Remove Topic Data
rm -rf $LOG_DIR/$TOPIC

# Remove Topic Metadata from ZooKeeper
#$KAFKA_HOME/bin/zookeeper-shell.sh 10.158.9.102:2181 <<EOF
$KAFKA_HOME/bin/zookeeper-shell.sh 10.158.9.201:2181 <<EOF

rmr /brokers/topics/$TOPIC
EOF

# Restart Kafka
$KAFKA_HOME/bin/kafka-server-start.sh -daemon $KAFKA_HOME/config/server.properties

echo "Topic $TOPIC cleanup completed!"

