import pandas as pd
from kafka import KafkaConsumer
import json
from scoring_function import ScoringFunction
from live_table import RecentPostsTable
import time
import datetime
from pytz import timezone
import threading

class Reporter:


def init_posts_table():
    # config variables
    topic_name = 'CLICK__FI_RECENT_POST__AG_COUNTS__EN_SCORE2'
    servers = 'ec2-100-20-18-195.us-west-2.compute.amazonaws.com:9092'
    # push_interval = datetime.timedelta(minutes=2)
    # connect to Kafka Topic.
    consumer = KafkaConsumer(topic_name,
                             bootstrap_servers=servers,
                             auto_offset_reset='earliest',
                             enable_auto_commit=True,
                             group_id='my-group',
                             value_deserializer=lambda x: json.loads(x.decode('utf-8')))
    scoring_function = ScoringFunction()
    return RecentPostsTable(consumer, scoring_function, datetime.timedelta(days = 1))


def update_posts(lock, live_posts_table):
    """
    Bottom feeder to update posts when no reports are scheduled.

    :param lock:
    :param live_posts_table:
    :return:
    """
    while True:
        lock.acquire()
        live_posts_table.update()
        lock.release()


def push_s3(min_interval: datetime.timedelta = datetime.timedelta(minutes = 2)):


if __name__ == "__main__":
    lock = threading.Lock() #lock for live_posts_table
    live_posts_table = init_posts_table()


