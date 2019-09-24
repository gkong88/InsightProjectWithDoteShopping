from flask import Flask, request
from typing import Dict, Callable, Optional, List, Text
import json
from flask_restful import Resource, Api
import confluent_kafka
import flatten_json
import pdb
import datetime

# Initialize central registry for your app
app = Flask(__name__)
api = Api(app)

# Ref: https://stackoverflow.com/questions/6999726/how-can-i-convert-a-datetime-object-to-milliseconds-since-epoch-unix-time-in-p
# How to convert time to epoch
epoch = datetime.datetime.utcfromtimestamp(0)
def segment_timestamp_to_unix_millis(segment_timestamp: str):
    """
    casts segment string timestamp to unix millis timestamp that Kafka supports
    """
    datetime.datetime.strptime()

    return int(( - epoch).total_seconds() * 1000)

class SegmentKafkaProducer:
    """
    Takes segment events and creates Kafka events
    """
    def __init__(self, kafkaConfig: str, topicJsonKey, keyJsonKey: str = None, keyTimestamp: str = None):
        self.topicJsonKey = topicJsonKey
        self.keyJsonKey = keyJsonKey
        self.keyTimestamp = keyTimestamp
        self.kafkaProducer = confluent_kafka.Producer(**kafkaConfig)

    def produce(self, jsonObject):
        """
        :param topicJsonKey: string of key that should be extracted from json to set kafka event topic
        :param keyJsonKey: string of key that should be extracted from json to set kafka event key
        :return:
        """
        flattenedJsonObject = flatten_json.flatten(json.loads(jsonObject))
        topic = ''.join(c for c in str(flattenedJsonObject[self.topicJsonKey])if c.isalnum()) + "_00_raw_flatJSON"

        key = flattenedJsonObject.get(self.keyJsonKey)
        if self.keyTimestamp is not None:
            timestamp = segment_timestamp_to_unix_millis(flattenedJsonObject.get(self.keyTimestamp))
        self.kafkaProducer.produce(topic, json.dumps(flattenedJsonObject), key, timestamp)

#TODO: refactor this later,
# consider https://stackoverflow.com/questions/19073952/flask-restful-how-to-add-resource-and-pass-it-non-global-data
kafkaBrokers = "ip-10-0-0-55.us-west-2.compute.internal:9092,ip-10-0-0-169.us-west-2.compute.internal:9092,ip-10-0-0-245.us-west-2.compute.internal:9092"
kafkaConfig = {'bootstrap.servers': kafkaBrokers}
kafkaProducer = SegmentKafkaProducer(kafkaConfig, "event", "properties_shoppable_post_id")

class SegmentRESTProxyForKafka(Resource):
    """
        REST API Sink for segment webhook that publishes data to kafka
    """
    def get(self):
        return "this is an endpoint for segment"

    def post(self):
        """
        creates kafka event from json object

        :return: none
        """
        print(request.data)
        kafkaProducer.produce(request.data)


if __name__ == '__main__':
    api.add_resource(SegmentRESTProxyForKafka, '/publishToKafka')
    app.run(debug = True)

