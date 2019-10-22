import pandas as pd
from kafka import KafkaConsumer, KafkaProducer
from scoring_function import ScoringFunction
import datetime
import time
import threading
from typing import Sequence
import json


class LiveTable:
    """
    LiveTable uses event sourcing on a "KTable" topic to reconstitute
    a full table for taking "snapshots" as reports.

    The constructor requires a KafkaConsumer to read "table updates" from.

    The windowing on the table is configurable.
    A user defined function can be supplied/updated that uses attributes
    of the post to create a new, derived column.
    """
    def __init__(self, input_topic_name: str,
                 bootstrap_servers: Sequence[str],
                 time_window_size=datetime.timedelta(days=3),
                 scoring_function=ScoringFunction()):
        """

        :param input_topic_name:
        :param bootstrap_servers:
        :param time_window_size:
        :param scoring_function:
        """
        self.producer = KafkaProducer(bootstrap_servers=bootstrap_servers, value_serializer=lambda x: json.dumps(x).encode('utf-8'))
        self.consumer = KafkaConsumer(input_topic_name,
                                      bootstrap_servers=list(bootstrap_servers),
                                      auto_offset_reset='earliest',
                                      enable_auto_commit=True,
                                      value_deserializer=lambda x: json.loads(x.decode('utf-8')))
        self.scoring_function = scoring_function
        self.time_window_size = time_window_size
        self.rolling_events_processed = 0
        self.rolling_sum_ingest_latency = 0
        self.rolling_sum_click_latency = 0
        self.time_window_start = None
        self.time_window_start_epoch = None
        self.topic_partition = None
        self.__seek_to_window_start() #initializes time_window_start, time_window_start_epoch, and topic_partition
        self.posts = {}
        self.__bulk_consume_new_events()
        self.scoring_function_lock = threading.Lock()

    def update(self):
        """
        Plays updates to the table from kafka topic.
        Purges table entries that are past their expiration date.
        Enriches all entries by applying scoring function

        :return:
        """
        self.__garbage_collect_old()
        self.__bulk_consume_new_events()

    def get_snapshot(self):
        """

        :return: return a copy of current state of table
        """
        self.__apply_score()
        return self.posts.copy()

    def update_scoring_function(self, scoring_function: ScoringFunction):
        """
        Updates scoring function for this table.
        Applies scoring function on all current entries in table

        :param scoring_function:
        :return:
        """
        self.scoring_function_lock.acquire()
        self.scoring_function = scoring_function
        self.scoring_function_lock.release()
        # self.__apply_score()

    def __apply_score(self):
        """
        Applies scoring function on all entries in table
        """
        self.scoring_function_lock.acquire()
        for key, json_dict in self.posts.items():
            json_dict['score'] = self.scoring_function.score(json_dict['PREVIEW'], json_dict['FULL_VIEW'])
            json_dict['coldness_score'] = self.scoring_function.coldness_score(json_dict['PREVIEW'])
            json_dict['hotness_score'] = self.scoring_function.hotness_score(json_dict['PREVIEW'], json_dict['FULL_VIEW'])
        self.scoring_function_lock.release()

    def __bulk_consume_new_events(self):
        """
        Reads kafka topic as an event source to reconstitute a "snapshot" of
        scores for all posts by replaying them into a dictionary.

        """
        end_offset = self.consumer.end_offsets([self.topic_partition])[self.topic_partition] - 1
        for m in self.consumer:
            if m is not None and m.value['POST_TIMESTAMP'] > self.time_window_start_epoch:
                self.posts[m.value['PROPERTIES_SHOPPABLE_POST_ID']] = m.value
                # self.__track_latency(m)
            if m.offset >= end_offset:
                break

    def __track_latency(self, m):
        if 'LAST_CLICK_TIMESTAMP' not in m.value or 'INGEST_TIMESTAMP' not in m.value:
            return
        click_timestamp = m.value['LAST_CLICK_TIMESTAMP']
        ingest_timestamp = m.value['INGEST_TIMESTAMP']
        if click_timestamp is None or ingest_timestamp is None:
            return
        now = round(time.time() * 1000)
        self.rolling_events_processed += 1
        self.rolling_sum_ingest_latency += now - ingest_timestamp
        self.rolling_sum_click_latency += now - click_timestamp
        if self.rolling_events_processed >= 1000:
            metrics = {'average_latency_ingest': self.rolling_sum_ingest_latency / self.rolling_events_processed,'average_latency_click': self.rolling_sum_click_latency / self.rolling_events_processed}
            self.producer.send(topic="average_latency", value=metrics)
            #self.producer.flush()
            self.rolling_events_processed = 0
            self.rolling_sum_ingest_latency = 0
            self.rolling_sum_click_latency = 0
            #print("===================================")
            #print("         PUSH LATENCY METRICS      ")
            #print(metrics)
            #print("===================================")

    def __garbage_collect_old(self):
        """
        Removes all expired table entries

        """
        for post_id in list(self.posts.keys()):
            if self.posts[post_id]['POST_TIMESTAMP'] < self.time_window_start_epoch:
                self.posts.pop(post_id)

    def __seek_to_window_start(self):
        """
        This function mutates the consumer to "seek" the kafka topic offset to that of the earliest event that
        is inside the time_window.
        """
        self.__update_time_window_start()
        if len(self.consumer.assignment()) == 0:
            # poll consumer to generate a topic partition assignment
            message = self.consumer.poll(1, 1)
            while len(message) == 0:
                message = self.consumer.poll(1, 1)
        self.topic_partition = self.consumer.assignment().pop()
        time_window_start_epoch = int(self.time_window_start.timestamp()*1000)

        # get first offset that is in the time window
        start_offset = self.consumer.offsets_for_times({self.topic_partition: time_window_start_epoch})[self.topic_partition].offset
        # set the consumer to consume from this offset
        self.consumer.seek(self.topic_partition, start_offset)

    def __update_time_window_start(self):
        """
        Returns start of time window from now - self.time_window_size.
        """
        self.time_window_start = datetime.datetime.now() - self.time_window_size
        self.time_window_start_epoch = int(self.time_window_start.timestamp() * 1000)
