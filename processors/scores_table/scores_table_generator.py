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
from utility import RepeatPeriodically, heartbeat, get_latest_message
from typing import Sequence


class Reporter:
    """
    Generates reports to a topic for s3 and a topic for ui at user defined intervals.

    Maintains a live table from updates with Kafka. Performs updates when no
    """
    # Concurrency is managed with monitor pattern (locking) on methods that
    # access the table state.
    def __init__(self, input_topic_name: str, bootstrap_servers: Sequence[str],
                 output_topic_name: str,
                 min_push_interval: datetime.timedelta = datetime.timedelta(seconds=1),
                 scoring_function_config: dict = ScoringFunction().get_config()
                 ):
        # cast bootstrap servers to list. needed for various fn preconditions
        self.bootstrap_servers = list(bootstrap_servers)
        # init lock for coordinating update and push events.
        self.lock = threading.Lock()
        self.threads = []

        # init producer for publishing scoring function state to UI
        self.output_topic_name = output_topic_name
        self.producer = KafkaProducer(bootstrap_servers=self.bootstrap_servers,
                                      value_serializer=lambda x: json.dumps(x).encode('utf-8'))

        # init live table
        self.table = LiveTable(input_topic_name, self.bootstrap_servers, datetime.timedelta(days=1))
        # apply scoring function, publish message containing these configs (for ui)
        self.update_scoring_function(scoring_function_config)

        # store timing variables
        self.min_push_interval = min_push_interval
        self.next_push_timestamp = datetime.datetime(1970, 1, 1)

        self.scoring_fn_config_topic = 'scores_config'
        self.listen_period_s = 3

    def run(self):
        """
        Starts processors.
        """
        # updates are always "enqueued"
        self.threads.append(threading.Thread(target=self.update_table_forever))
        # snapshots are "enqueued" periodically.
        # uses lock management to guarantee it only needs to wait
        # at most 1 update cycle before pushing
        self.threads.append(threading.Thread(target=self.push_snapshot_forever))
        # listens for config changes. if an update appears in config change topic
        # locks the state of this instance and updates the scoring function.
        self.threads.append(threading.Thread(target=self.listen_for_config_changes_forever))
        for thread in self.threads:
            thread.start()

    def update_scoring_function(self, scoring_function_config: dict):
        """
        Updates config file.
        Publishes event that function has been updated.

        :param scoring_function_config: ScoringFunction config
        :return:
        """
        self.lock.acquire()
        scoring_function = ScoringFunction(**scoring_function_config)
        self.table.update_scoring_function(scoring_function)
        self.producer.send(topic=self.scoring_fn_config_topic, key='register', value=scoring_function_config)
        self.producer.flush()
        self.lock.release()

    def update_table_forever(self):
        """
        Update table when no reports are scheduled.
        Bottomfeeder that is always active.

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
        """
        Periodically publishes snapshot of table to kafka topic
        """
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
            self.producer.send(self.topic)
            self.next_push_timestamp = datetime.datetime.now() + self.min_push_interval
            print('pushed')

    def listen_for_config_changes_forever(self):
        """
        Polls for config update messages.

        When a new message is recieved, locks instance state and applies update.
        """
        while True:
            time.sleep(self.listen_period)
            msg = get_latest_message(input_topic_name=self.scoring_fn_config_topic)
            if msg.key != 'register':
                self.lock.acquire()
                self.update_scoring_function(msg.value)
                self.lock.release()


if __name__ == "__main__":
    input_topic_name = 'CLICK__FI_RECENT_POST__AG_COUNTS'
    bootstrap_servers = ['ec2-100-20-18-195.us-west-2.compute.amazonaws.com:9092',
                     'ec2-100-20-8-59.us-west-2.compute.amazonaws.com:9092',
                     'ec2-100-20-75-14.us-west-2.compute.amazonaws.com:9092']
    output_topic_name = "recent_posts_scores_snapshot"

    heartbeat_kwargs = {'bootstrap_servers': bootstrap_servers, 'topic_name': 'heartbeat_table_generator'}
    RepeatPeriodically(fn=heartbeat, interval=120, kwargs=heartbeat_kwargs).run()

    reporter = Reporter(input_topic_name= input_topic_name,
                        bootstrap_servers= bootstrap_servers,
                        output_topic_name = output_topic_name)
    reporter.run()


