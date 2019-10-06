import pandas as pd
from kafka import KafkaConsumer
import json
from scoring_function import ScoringFunction
import datetime
import threading
import time
import os, sys
sys.path.insert(0, os.path.abspath('../util'))
from utility import RepeatPeriodically, heartbeat

class S3SinkConnector:
    def __init__(self, input_topic_name: str, kafka_servers: str,
                 min_push_interval: datetime.timedelta = datetime.timedelta(minutes = 2),

                 ):
        # init kafka consumer
        consumer = KafkaConsumer(input_topic_name,
                                 bootstrap_servers=kafka_servers,
                                 auto_offset_reset='earliest',
                                 enable_auto_commit=True,
                                 value_deserializer=lambda x: json.loads(x.decode('utf-8')))
        # init live table
        self.table = LiveTable(consumer, datetime.timedelta(days = 1), scoring_function)

        # store timing variables
        self.min_push_interval = min_push_interval
        self.next_push_timestamp = datetime.datetime(1970, 1, 1)

        # init lock for coordinating update and push events.
        self.lock = threading.Lock()
        self.threads = []

        self.destination_url = kafka_rest_proxy_server + "/topics/" + output_topic_name
        self.headers = {"Content-Type": "application/vnd.kafka.json.v2+json", "Accept": "application/vnd.kafka.v2+json",
                   "Connection": 'close'}

def get_latest():
    pass

def push_s3(self):

if __name__ == "__main__":
    topic_name = 'CLICK__FI_RECENT_POST__AG_COUNTS'
    kafka_servers = ['ec2-100-20-18-195.us-west-2.compute.amazonaws.com:9092',
                     'ec2-100-20-8-59.us-west-2.compute.amazonaws.com:9092',
                     'ec2-100-20-75-14.us-west-2.compute.amazonaws.com:9092']
    heartbeat_kwargs = {'bootstrap_servers': kafka_servers, 'topic_name': 'pipeline_logs', 'key': 'conn_s3_sink'}
    RepeatPeriodically(fn=heartbeat, interval=300, kwargs=heartbeat_kwargs).run()