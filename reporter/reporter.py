import pandas as pd
from kafka import KafkaConsumer
import json
from scoring_function import ScoringFunction
from live_table import LiveTable
import datetime
import threading
import requests
import time

class Reporter:
    """
    Generates reports to a topic for s3 and a topic for ui at user defined intervals.

    Maintains a live table from updates with Kafka. Performs updates when no
    """
    # Concurrency is managed with monitor pattern (locking) on methods that
    # access the table state.
    def __init__(self, topic_name: str, kafka_servers: str,
                 output_topic_name: str, kafka_rest_proxy_server: str,
                 scoring_function: ScoringFunction = ScoringFunction(),
                 min_push_interval: datetime.timedelta = datetime.timedelta(seconds = 2)
                 ):
        # init kafka consumer
        consumer = KafkaConsumer(topic_name,
                                 bootstrap_servers=kafka_servers,
                                 auto_offset_reset='earliest',
                                 enable_auto_commit=True,
                                 group_id='my-group',
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

    def run(self):
        self.threads.append(threading.Thread(target = self.update_table_forever))
        # self.threads.append(threading.Thread(self.push_s3_forever))
        self.threads.append(threading.Thread(target = self.push_snapshot_forever))
        for thread in self.threads:
            thread.start()

    def update_scoring_function(self):
        # TODO: add functionality to incorporate function updates
        # TODO: add listener for function updates that calls this
        # TODO:
        pass

    def update_table_forever(self):
        """
        Bottom feeder to update posts when no reports are scheduled.

        :param lock:
        :param live_posts_table:
        :return:
        """
        # TODO: bottom feeder is actually a first class citizen
        # Refactor to prioritize lock requests on s3 -> ui -> posts, respectively.
        while True:
            self.lock.acquire()
            self.table.update()
            self.lock.release()
            print("updated")

    # def push_s3_forever(self):
    #     while True:
    #         self.lock.acquire()
    #         df = self.table.get_snapshot()
    #         self.lock.release()

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
            print("pushed snapshot")

    # def push_snapshot(self, min_interval: datetime.timedelta = datetime.timedelta(minutes = 2)):
    #     # TODO
    #     pass
    #
    # def push_s3(self, min_interval: datetime.timedelta = datetime.timedelta(minutes = 2)):
    #     # TODO
    #     pass


if __name__ == "__main__":
    topic_name = 'CLICK__FI_RECENT_POST__AG_COUNTS__EN_SCORE2'
    kafka_servers = 'ec2-100-20-18-195.us-west-2.compute.amazonaws.com:9092,ec2-100-20-8-59.us-west-2.compute.amazonaws.com:9092,ec2-100-20-75-14.us-west-2.compute.amazonaws.com:9092'
    output_topic_name = "recent_posts_scores_snapshot"
    kafka_rest_proxy_server = "http://ec2-52-36-231-83.us-west-2.compute.amazonaws.com:8082"
    reporter = Reporter(topic_name = topic_name,
                        kafka_servers = kafka_servers,
                        output_topic_name = output_topic_name,
                        kafka_rest_proxy_server = kafka_rest_proxy_server)
    reporter.run()


