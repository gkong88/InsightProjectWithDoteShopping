# InsightProjectWithDoteShopping
Project for Dote Shopping

## Repo directory structure

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
    ├── reporter
    |   ├── README.md    
    └── ui
        ├── README.md    
        └── products.csv
        └── order_products.csv
