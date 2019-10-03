#!/bin/sh

sudo mkdir -p /etc/ksql/ext
sudo cp ./target/'ksql-udf-demo-1.0.jar' /etc/ksql/ext/'ksql-udf-demo-1.0.jar'
sudo cp ./target/'ksql-udf-demo-1.0-jar-with-dependencies.jar' /etc/ksql/ext/'ksql-udf-demo-1.0-jar-with-dependencies.jar'
