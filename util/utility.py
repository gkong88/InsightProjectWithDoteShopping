from kafka import KafkaProducer, KafkaConsumer, TopicPartition
from typing import Callable
import threading
import time


class RepeatPeriodically:
    """
    Class that periodically executes a function at a specified interval
    """
    def __init__(self, fn: Callable, interval: float, kwargs: dict):
        """

        :param fn: function to be periodically executed
        :param interval: interval in seconds
        :param kwargs: keyword arguments for function
        """
        self.interval = interval
        self.fn = fn
        self.kwargs = kwargs

    def run(self):
        self.fn(**self.kwargs)
        threading.Timer(self.interval, self.run).start()


def heartbeat(bootstrap_servers, topic_name):
    """
    Sends a heartbeat to a kafka topic.
    Used for logging

    :param bootstrap_servers: addresses of kafka bootstrap servers
    :param topic_name:
    :return:
    """
    p = KafkaProducer(bootstrap_servers=bootstrap_servers)
    p.send(topic=topic_name, key=b'ping')
    p.flush()
    p.close()


def get_latest_message(input_topic_name: str, config: dict):
    """

    :param input_topic_name:
        REQUIRES only one partition for global ordering
    :return:
    """
    # create consumer for topic
    consumer = KafkaConsumer(**config)
    partition_number = list(consumer.partitions_for_topic(input_topic_name))[0]
    topic_partition = TopicPartition(input_topic_name, partition_number)
    consumer.assign([topic_partition])

    # get latest message
    consumer.seek(topic_partition, consumer.end_offsets([topic_partition])[topic_partition] - 1)
    message = consumer.poll(3000, 1)[topic_partition][0]

    # close connection and return result
    consumer.close()
    return message
