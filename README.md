![title](imgs/title.PNG?raw=true "Data Pipeline")
# Million-Armed Bandit

This repo constructs a data pipeline for real-time ranking of posts based on user click streams. 

It's intended use case is for improving content feeds, specifically for exploring new posts vs exploiting well performing posts. 
It can be thought of as a N-Armed Bandit Problem

It is implemented using a Lambda architecture (it is used to supplement, not replace existing batch analytics) for Dote Shopping.

### Motivation

Content feed ranking balances exploitation of already popular posts, vs exploration of new posts. 
Exploitation of popular posts means missing out on new, fresh content, from new creators. 
Over correcting with "cold starting" new posts into the feed for too long, means wasting user attention on
content that can be inferred as poor quality. 

### Solution

Real-time analytics can be used to manage this balance in a more responsive, efficient way.

For each post, the streaming platform maintains a running aggregate of **previews** (number of times a post has been shown to users)
and **clicks**. These statistics are used to calculate a score and is pushed to the backend scoring system for ranking.

The score is broken into two components: 
* Coldness is inversely proportional with **previews** 
* Hotness is proportional to **click thorugh rate** (clicks / previews) 

Parameters of the scoring function are exposed, and configurable, through the UI (sigmoid steepness and thresholds, max cold score, max hot score). 

![Scoring_Algorithm](imgs/scoring_function_graph.PNG?raw=true "Scoring Algorithm")
*Scoring Function* 


### Design

Segment, a managed service for collecting customer interaction data, delivers relevant events as JSONs to a webserver that
ingests the events into Kafka, the streaming platform. Relevant events are:

* Post Creation
* Post Preview
* Post Click

KSQL is used for computing real-time aggregates.

Aggregates are used by a python application that applies scoring function and sends snapshots to consumers.

![data_pipeline](imgs/data_pipeline.PNG?raw=true "Data Pipeline")
*Data Pipeline*

## Setup

See respective README's for setup instructions.

+ [Kafka](https://github.com/gkong88/InsightProjectWithDoteShopping/tree/master/config)
+ [KSQL](https://github.com/gkong88/InsightProjectWithDoteShopping/tree/master/processors/ksql)
+ [Segment Source](https://github.com/gkong88/InsightProjectWithDoteShopping/tree/master/connectors)
+ [Python Processor](https://github.com/gkong88/InsightProjectWithDoteShopping/tree/master/processors/scores_table)
+ [S3 Sink](https://github.com/gkong88/InsightProjectWithDoteShopping/tree/master/connectors)
+ [Dashboard](https://github.com/gkong88/InsightProjectWithDoteShopping/tree/master/ui) 
  

### Misc Links

* [UI](http://cleardata.club)
* [Presentation](https://docs.google.com/presentation/d/1X8pTTB6mPH0ciCwkJ0ja58ogYM26Bq7wKbcdxdAAaBk/)