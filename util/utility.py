from kafka import KafkaProducer, KafkaConsumer, TopicPartition
from typing import Callable
import threading
import json


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


def get_latest_message(input_topic_name: str,
                       config: dict = {'bootstrap_servers': ['ec2-100-20-18-195.us-west-2.compute.amazonaws.com:9092',
                                                             'ec2-100-20-8-59.us-west-2.compute.amazonaws.com:9092',
                                                             'ec2-100-20-75-14.us-west-2.compute.amazonaws.com:9092'],
                                       'auto_offset_reset': 'earliest',
                                       'enable_auto_commit': True,
                                       'value_deserializer':  lambda x: json.loads(x.decode('utf-8'))}):
    """

    :param input_topic_name: Kafka topic name to read from
        REQUIRES only one partition for global ordering
    :param config: KafkaConsumer configs
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
