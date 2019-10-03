import pandas as pd
from kafka import KafkaConsumer
from scoring_function_creator import ScoringFunctionCreator
import datetime


class RecentPostsTable:
    def __init__(self, consumer: KafkaConsumer,
                 scoring_function=ScoringFunctionCreator(),
                 time_window_size=datetime.timedelta(days=3)):
        self.consumer = consumer
        self.scoring_function = scoring_function
        self.time_window_size = time_window_size
        self.time_window_start = None
        self.time_window_start_epoch = None
        self.topic_partition = None
        self.__seek_to_window_start() #initializes time_window_start, time_window_start_epoch, and topic_partition
        self.posts = {}
        self.__bulk_consume_events()

    def get_snapshot(self) -> pd.DataFrame:
        """

        :return:
        """
        self.__garbage_collect_old()
        self.__bulk_consume_events()
        self.__apply_score()
        return pd.DataFrame.from_dict(self.posts, orient='index')

    def update_scoring_function(self, scoring_function):
        self.scoring_function = scoring_function
        self.__apply_score()

    def __apply_score(self):
        for key, json_dict in self.posts.items():
            json_dict['score'] = self.scoring_function.score(json_dict['PREVIEW'], json_dict['FULL_VIEW'])
            json_dict['coldness_score'] = self.scoring_function.coldness_score(json_dict['PREVIEW'])
            json_dict['hotness_score'] = self.scoring_function.hotness_score(json_dict['PREVIEW'], json_dict['FULL_VIEW'])

    def __bulk_consume_events(self):
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
        #TODO: Refactor with a secondary index, ordered by creation timestamp.
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
