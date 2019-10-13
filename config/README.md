This folder contains config files for nodes in the kafka cluster.

The paths to each config file from this directory is the same relative path to where it should be located in the respective nodes.

## Setup

Requirements: 
+ Java 8 SDK
+ [Confluent Platform](https://docs.confluent.io/current/installation/installing_cp/deb-ubuntu.html#systemd-ubuntu-debian-install)


## Loading config files

Run loads_config.sh to load all kafka (and related services) config files into their default locations for confluent kafka.


Usage:
sudo bash ./load_configs.sh

#TODO: 
    for handoff, write a script that generates ALL topics used with the corrent number of partitions needed. 
	
