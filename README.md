# Million-Armed Bandit

## Project Overview

## Solution

## Algorithm

## Tech Stack

The directory structure for your repo should look like this:

    ├── README.md
    ├── config
    |   ├── README.md
    |   ├── load_configs.sh
    |   └── etc
    |       ├── confluent-control-center
    |       |   └── control-center-production.properties
    |       ├── kafka-rest
    |       |   └── kafka-rest.properties      
    |       ├── kafka
    |       |   ├── connect-distributed.properties     
    |       |   ├── server.properties   
    |       |   └── zookeeper.properties                 
    |       ├── ksql
    |       |   └── ksql-server.properties 
    |       └── schema-registry
    |           └── schema-registry.properties                   
    ├── ingestor
    |   ├── README.md    
    |   ├── ingestor.py
    |   └── wsgi.py
    ├── ksql
    |   ├── README.md
    |   ├── ksql_playbook.sql
    |   └── udf
    |       |── README.md
    |       |── pom.xml
    |       └── src/main/java/com/dote/ksql/udf
    |           └── RTScoring.properties                       
    ├── reporter
    |   ├── README.md    
    |   ├── report_generator.py
    |   ├── living_table.py
    |   └── scoring_function.py
    └── ui
        ├── README.md    
        └── ui.py
        └── wsgi.py

