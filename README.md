# Million-Armed Bandit

This repo constructs a data pipeline for real-time ranking of posts based on user click streams. 

It's intended use case is for improving content feeds, specifically regarding exploring new posts vs exploiting well performing posts. It can be thought of as a N-Armed Bandit Problem

It is implemented using a Lambda architecture (it is used to supplement, not replace existing batch analytics).

### Motivation

Content feed ranking balances exploitation of already popular posts, vs exploration of new posts. Exploitation of popular posts means missing out on new, fresh content, from new creators. Over correcting with "cold starting" new posts into the feed for too long, means wasting user attention on what can now be inferred. 

### Solution

Real-time analytics can be used to manage this balance in a more responsive, efficient way.

### Algorithm

### Tech Stack

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

