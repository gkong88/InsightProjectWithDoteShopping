This folder contains config files for nodes in your kafka cluster.

The paths to each config file from this directory is the same relative path to where it should be located in the respective nodes.

Run loads_config.sh to load all kafka (and related services) config files into their default locations for confluent kafka.


Installed on Ubunutu 18.04 AMI.

Java 8 install: 
sudo apt update
sudo apt install openjdk-8-jdk

Confluent Platform (including Kafka) Install:
https://docs.confluent.io/current/installation/installing_cp/deb-ubuntu.html#systemd-ubuntu-debian-install


