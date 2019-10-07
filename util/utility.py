from kafka import KafkaProducer
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
