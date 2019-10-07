from kafka import KafkaConsumer, KafkaProducer
import json
from scoring_function import ScoringFunction
from live_table import LiveTable
import datetime
import threading
import requests
import time
import os, sys
sys.path.insert(0, os.path.abspath('../../util'))
from utility import RepeatPeriodically, heartbeat

class Reporter:
    """
    Generates reports to a topic for s3 and a topic for ui at user defined intervals.

    Maintains a live table from updates with Kafka. Performs updates when no
    """
    # Concurrency is managed with monitor pattern (locking) on methods that
    # access the table state.
    def __init__(self, topic_name: str, bootstrap_servers: str,
                 output_topic_name: str, kafka_rest_proxy_server: str,
                 min_push_interval: datetime.timedelta = datetime.timedelta(seconds=1),
                 scoring_function_config: dict = ScoringFunction().get_config()
                 ):
        # init lock for coordinating update and push events.
        self.lock = threading.Lock()
        self.threads = []

        # init producer for publishing scoring function state to UI
        self.producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                                      value_serializer=lambda x: json.dumps(x).encode('utf-8'))
        # init kafka consumer
        self.consumer = KafkaConsumer(topic_name,
                                      bootstrap_servers=bootstrap_servers,
                                      auto_offset_reset='earliest',
                                      enable_auto_commit=True,
                                      value_deserializer=lambda x: json.loads(x.decode('utf-8')))
        # init live table
        self.table = LiveTable(self.consumer, datetime.timedelta(days = 1))
        # apply scoring function, publish message containing these configs (for ui)
        self.update_scoring_function(scoring_function_config)

        # store timing variables
        self.min_push_interval = min_push_interval
        self.next_push_timestamp = datetime.datetime(1970, 1, 1)

        self.destination_url = kafka_rest_proxy_server + "/topics/" + output_topic_name
        self.headers = {"Content-Type": "application/vnd.kafka.json.v2+json", "Accept": "application/vnd.kafka.v2+json",
                   "Connection": 'close'}

    def run(self):
        """
        Starts processors.
        """
        # updates are always "enqueued"
        self.threads.append(threading.Thread(target = self.update_table_forever))
        # snapshots are "enqueued" periodically.
        # uses lock management to guarantee it only needs to wait
        # at most 1 update cycle before pushing
        self.threads.append(threading.Thread(target = self.push_snapshot_forever))
        for thread in self.threads:
            thread.start()

    def update_scoring_function(self, scoring_function_config: dict):
        # TODO: add listener for function updates that calls this
        self.lock.acquire()
        scoring_function = ScoringFunction(**scoring_function_config)
        self.table.update_scoring_function(scoring_function)
        producer = KafkaProducer(bootstrap_servers=self.bootstrap_servers,
                                 value_serializer=lambda x: json.dumps(x).encode('utf-8'))
        producer.send(topic = 'scores_config', key ='register', value=scoring_function_config)
        producer.close()
        self.lock.release()

    def update_table_forever(self):
        """
        Update table when no reports are scheduled.

        :param lock:
        :param live_posts_table:
        :return:
        """
        while True:
            self.lock.acquire()
            self.table.update()
            self.lock.release()
            print('updated')

    def push_snapshot_forever(self):
        while True:
            self.lock.acquire()
            posts = self.table.get_snapshot()
            self.lock.release()

            while datetime.datetime.now() < self.next_push_timestamp:
                sleep_duration = max((self.next_push_timestamp - datetime.datetime.now()).seconds, 1)
                time.sleep(sleep_duration)

            kafka_payload = {"records": [{"value": posts}]}
            response = requests.post(self.destination_url, json=kafka_payload, headers=self.headers)
            response.raise_for_status()
            response.close()
            self.next_push_timestamp = datetime.datetime.now() + self.min_push_interval
            print('pushed')


if __name__ == "__main__":
    input_topic_name = 'CLICK__FI_RECENT_POST__AG_COUNTS'
    kafka_servers = ['ec2-100-20-18-195.us-west-2.compute.amazonaws.com:9092',
                     'ec2-100-20-8-59.us-west-2.compute.amazonaws.com:9092',
                     'ec2-100-20-75-14.us-west-2.compute.amazonaws.com:9092']
    output_topic_name = "recent_posts_scores_snapshot"
    kafka_rest_proxy_server = "http://ec2-52-36-231-83.us-west-2.compute.amazonaws.com:8082"

    heartbeat_kwargs = {'bootstrap_servers': kafka_servers, 'topic_name': 'heartbeat_table_generator'}
    RepeatPeriodically(fn=heartbeat, interval=120, kwargs=heartbeat_kwargs).run()

    reporter = Reporter(topic_name = input_topic_name,
                        bootstrap_servers= kafka_servers,
                        output_topic_name = output_topic_name,
                        kafka_rest_proxy_server = kafka_rest_proxy_server)
    reporter.run()


