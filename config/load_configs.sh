#!/bin/sh

# this file copies all kafka config files relative to this directory to the local machine
sudo cp ./etc/schema-registry/schema-registry.properties /etc/schema-registry/schema-registry.properties
sudo cp ./etc/confluent-control-center/control-center-production.properties /etc/confluent-control-center/control-center-production.properties
sudo cp ./etc/ksql/ksql-server.properties /etc/ksql/ksql-server.properties
sudo cp ./etc/kafka/connect-distributed.properties /etc/kafka/connect-distributed.properties
sudo cp ./etc/kafka/zookeeper.properties /etc/kafka/zookeeper.properties
sudo cp ./etc/kafka/server.properties /etc/kafka/server.properties

sudo rm -rf /var/lib/kafka-streams
sudo mkdir /var/lib/kafka-streams
sudo chmod 777 -R /var/lib/kafka-streams
