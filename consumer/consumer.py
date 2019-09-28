# from gevent.pywsgi import WSGIServer
# from flask import Flask, request
import json
from flask_restful import Resource, Api
from confluent_kafka import Consumer, KafkaException
import sys
import pdb
# import datetime

class Payload:
    """
    Data structure
    """
    def __init__(self, window: int = 120):
        pass

def push_to_s3(real_time_scores_csv: str):
    """

    :param real_time_scores_csv:
    :return: 0 if successful, 1 if failure
    """
    return 1

def push_to_dash(real_time_scores_csv: str):
    return 1

config = {'bootstrap.servers': "ec2-100-20-75-14.us-west-2.compute.amazonaws.com:9092,ec2-54-218-57-94.us-west-2.compute.amazonaws.com:9092,ec2-34-220-177-171.us-west-2.compute.amazonaws.com:9092",
           'group.id': 1000,
           'auto.offset.reset': 'earliest'}
consumer = Consumer(config)
consumer.subscribe(topics = ['trackViewedShoppableFit_00_raw_flatJSON'])
try:
    while True:
        msg = consumer.poll(timeout = 1.0)
        if msg is None:
            continue
        elif msg.error():
            raise KafkaException(msg.error())
        else:
            print("consume!")
            print(msg)
except KeyboardInterrupt:
    sys.stderr.write('%% Aborted by user\n')
finally:
    consumer.close()

# TODO: you are counting on log drop off to window by 1 week!
# are your timestamps still set as the segment date???

# TODO:
## are you replaying a lot of redundant info? check with pdb




