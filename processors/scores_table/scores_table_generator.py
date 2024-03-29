from kafka import KafkaProducer, KafkaConsumer
import json
from scoring_function import ScoringFunction
from live_table import LiveTable
import datetime
import threading
import time
import os, sys
sys.path.insert(0, os.path.abspath('../../util'))
from utility import RepeatPeriodically, heartbeat, get_latest_message
from typing import Sequence
import pdb


class Reporter:
    """
    Reporter has four functions.

    1) Maintains dynamic table state based on table updates from Kafka Tables (KSQL).
    2) Periodically pushes snapshots of the table to a Kafka topic.
    3) Listens to a topic for config updates (scoring function) and applies them when necessary
    """
    # Concurrency is managed with monitor pattern (locking) on methods that
    # access the table state.
    def __init__(self,
                 bootstrap_servers: Sequence[str],
                 input_table_updates_topic_name: str,
                 output_snapshot_topic_name: str,
                 scores_config_running_topic_name: str = 'scores_config_running',
                 scores_config_update_topic_name: str = 'scores_config_update',
                 scoring_function_config: dict = ScoringFunction().get_config(),
                 interval_snapshot_s: int = 1,
                 interval_listen_config_update_s: int = 1
                 ):
        """
        :param bootstrap_servers: bootstrap servers for kafka service discovery
        :param input_table_updates_topic_name: kafka topic to listen for table updates (KSQL)
        :param output_snapshot_topic_name: kafka topic to publish table snapshots
        :param scores_config_running_topic_name: kafka topic to register scoring fn used (to ui)
        :param scores_config_update_topic_name: kafka topic to listen for scoring fn changes (from ui)
        :param scoring_function_config: initial scoring function to use for table
        :param interval_snapshot_s: time interval to generate table snapshots
        :param interval_listen_config_update_s: time interval to listen for config updates (from ui)
        """
        # cast bootstrap servers to list. needed for downstream func param preconditions
        self.bootstrap_servers = list(bootstrap_servers)
        self.input_table_updates_topic_name = input_table_updates_topic_name
        self.output_topic_name = output_snapshot_topic_name
        self.scores_config_running_topic_name = scores_config_running_topic_name
        self.scores_config_update_topic_name = scores_config_update_topic_name

        # store timing variables
        self.min_push_interval = datetime.timedelta(seconds=interval_snapshot_s)
        self.listen_period_s = interval_listen_config_update_s
        self.next_push_timestamp = datetime.datetime(1970, 1, 1)

        # init lock for coordinating update and push events.
        self.table_state_lock = threading.Lock()
        self.threads = []

        # init producer for publishing snapshots
        self.producer = KafkaProducer(bootstrap_servers=self.bootstrap_servers,
                                      value_serializer=lambda x: json.dumps(x).encode('utf-8'))
        # init live table to hold dynamic state
        self.table = LiveTable(self.input_table_updates_topic_name, self.bootstrap_servers, datetime.timedelta(days=1))
        # apply scoring function, publish message containing these configs (for ui)
        self.__update_scoring_function(scoring_function_config)

    def run(self):
        """
        Starts processors.
        """
        # updates are always "enqueued"
        self.threads.append(threading.Thread(target=self.__run_table_update_forever))
        # snapshots are "enqueued" periodically.
        # uses lock management to guarantee it only needs to wait
        # at most 1 update cycle before pushing
        self.threads.append(threading.Thread(target=self.__run_push_snapshot_forever))
        # listens for config changes. if an update appears in config change topic
        # locks the state of this instance and updates the scoring function.
        self.threads.append(threading.Thread(target=self.__run_listen_for_config_changes_forever))
        for thread in self.threads:
            thread.start()

    def __run_table_update_forever(self):
        """
        Update table when no reports are scheduled.
        Bottomfeeder that is always active.

        :param lock:
        :param live_posts_table:
        :return:
        """
        while True:
            print("updater requesting lock")
            self.table_state_lock.acquire()
            print("updater has lock")
            self.table.update()
            self.table_state_lock.release()
            print("updater releases lock")
            print('updated')

    def __run_push_snapshot_forever(self):
        """
        Periodically publishes snapshot of table to kafka topic
        """
        while True:
            print('pusher requesting lock')
            self.table_state_lock.acquire()
            print('pusher has lock')
            posts = self.table.get_snapshot()
            self.table_state_lock.release()
            print('pushed, releasing lock')

            while datetime.datetime.now() < self.next_push_timestamp:
                sleep_duration = max((self.next_push_timestamp - datetime.datetime.now()).seconds, 1)
                time.sleep(sleep_duration)
            self.producer.send(topic=self.output_topic_name, value=posts)
            self.producer.flush()
            self.next_push_timestamp = datetime.datetime.now() + self.min_push_interval

    def __run_listen_for_config_changes_forever(self):
        """
        Polls for config update messages.

        When a new message is recieved, locks instance state and applies update.
        """
        # c = KafkaConsumer(topics=[self.scores_config_update_topic_name],
        configs = {'bootstrap_servers': ['ec2-100-20-18-195.us-west-2.compute.amazonaws.com:9092',
                                         'ec2-100-20-8-59.us-west-2.compute.amazonaws.com:9092',
                                         'ec2-100-20-75-14.us-west-2.compute.amazonaws.com:9092'],
                                         'group_id': 1,
                   'auto_offset_reset': 'latest',
                   'enable_auto_commit': True,
                   'value_deserializer': lambda x: json.loads(x.decode('utf-8'))}
        c = KafkaConsumer(**configs)
        c.subscribe(self.scores_config_update_topic_name)

        while True:
            msgs = c.poll(float("Inf"))
            if len(msgs) == 0:
                continue
            p = list(c.assignment())[-1]
            m = list(msgs.values())[-1][-1]
            print(dir(m))
            print("===================================")
            print("Config change received: %s"%m.value)
            self.__update_scoring_function(m.value)
            print("Config change affected")
            print("===================================")
            #print(m.partition)
            #print(dir(m.partition))
            #partitions_for_topic_set = c.partitions_for_topic(self.scores_config_running_topic_name)
            #if partitions_for_topic_set is None:
            #    continue
            #p = list(partitions_for_topic_set)[-1]
            #print(p)
            #print(partitions_for_topic_set)
            #c.commit({p: m.offset})
            #pdb.set_trace()
            #c.commit({p: m.offset})
            #c.seek_to_end(p)

    def __update_scoring_function(self, scoring_function_config: dict):
        """
        Updates config file.
        Publishes event that function has been updated.

        :param scoring_function_config: ScoringFunction config
        :return:
        """
        # guarantees mutual exclusion over methods that USE the scoring function.
        # self.lock.acquire()
        scoring_function = ScoringFunction(**scoring_function_config)
        self.table.update_scoring_function(scoring_function)
        self.producer.send(topic=self.scores_config_running_topic_name, value=scoring_function_config)
        self.producer.flush()
        # self.lock.release()


if __name__ == "__main__":
    # init heartbeat log for this process
    bootstrap_servers = ['ec2-100-20-18-195.us-west-2.compute.amazonaws.com:9092',
                         'ec2-100-20-8-59.us-west-2.compute.amazonaws.com:9092',
                         'ec2-100-20-75-14.us-west-2.compute.amazonaws.com:9092']
    heartbeat_kwargs = {'bootstrap_servers': bootstrap_servers, 'topic_name': 'heartbeat_table_generator'}
    RepeatPeriodically(fn=heartbeat, interval=120, kwargs=heartbeat_kwargs).run()

    # init reporter and run
    input_table_updates_topic_name = 'CLICK__FI_RECENT_POST__AG_COUNTS'
    output_snapshot_topic_name = "recent_posts_scores_snapshot"
    reporter = Reporter(input_table_updates_topic_name=input_table_updates_topic_name,
                        bootstrap_servers=bootstrap_servers,
                        output_snapshot_topic_name=output_snapshot_topic_name)
    reporter.run()


