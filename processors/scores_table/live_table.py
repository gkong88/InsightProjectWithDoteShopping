import pandas as pd
from kafka import KafkaConsumer
from scoring_function import ScoringFunction
import datetime
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
        self.consumer = KafkaConsumer(input_topic_name,
                                      bootstrap_servers=list(bootstrap_servers),
                                      auto_offset_reset='earliest',
                                      enable_auto_commit=True,
                                      value_deserializer=lambda x: json.loads(x.decode('utf-8')))
        self.scoring_function = scoring_function
        self.time_window_size = time_window_size
        self.time_window_start = None
        self.time_window_start_epoch = None
        self.topic_partition = None
        self.__seek_to_window_start() #initializes time_window_start, time_window_start_epoch, and topic_partition
        self.posts = {}
        self.bulk_consume_events()

    def update(self):
        self.__garbage_collect_old()
        self.bulk_consume_events()
        self.__apply_score()

    def get_snapshot(self):
        """

        :return: return a copy of current state of table
        """
        return self.posts.copy()

    def update_scoring_function(self, scoring_function: ScoringFunction):
        """
        Updates scoring function for this table. Reapplies function to all rows.

        :param scoring_function:
        :return:
        """
        self.scoring_function = scoring_function
        self.__apply_score()

    def __apply_score(self):
        for key, json_dict in self.posts.items():
            json_dict['score'] = self.scoring_function.score(json_dict['PREVIEW'], json_dict['FULL_VIEW'])
            json_dict['coldness_score'] = self.scoring_function.coldness_score(json_dict['PREVIEW'])
            json_dict['hotness_score'] = self.scoring_function.hotness_score(json_dict['PREVIEW'], json_dict['FULL_VIEW'])

    def bulk_consume_events(self):
        """
        Reads kafka topic as an event source to reconstitute a "snapshot" of
        scores for all posts by replaying them into a dictionary.

        """
        end_offset = self.consumer.end_offsets([self.topic_partition])[self.topic_partition] - 1
        for m in self.consumer:
            if m is not None and m.value['POST_TIMESTAMP'] > self.time_window_start_epoch:
                self.posts[m.value['PROPERTIES_SHOPPABLE_POST_ID']] = m.value
            if m.offset >= end_offset:
                break

    def __garbage_collect_old(self):
        """
        Removes all tracked posts
        :return:
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
