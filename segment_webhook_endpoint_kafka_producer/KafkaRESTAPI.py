from flask import Flask, request
from typing import Dict, Callable, Optional, List, Text
import json
from flask_restful import Resource, Api
import confluent_kafka
import flatten_json
import pdb

# Initialize central registry for your app
app = Flask(__name__)
api = Api(app)

class SegmentKafkaProducer:
    """
    Takes segment events and creates Kafka events
    """
    def __init__(self, kafkaConfig: str, topicJsonKey, keyJsonKey: str = None):
        self.topicJsonKey = topicJsonKey
        self.keyJsonKey = keyJsonKey
        self.kafkaProducer = confluent_kafka.Producer(**kafkaConfig)

    def sendKafkaMessge(self, jsonObject):
        """
        :param topicJsonKey: string of key that should be extracted from json to set kafka event topic
        :param keyJsonKey: string of key that should be extracted from json to set kafka event key
        :return:
        """
        flattenedJsonObject = flatten_json.flatten(json.loads(jsonObject))
        topic = flattenedJsonObject[self.topicJsonKey]
        if self.keyJsonKey != None:
            key = flattenedJsonObject[self.keyJsonKey]
        self.kafkaProducer.produce(topic, json.dumps(flattenedJsonObject), str(key))

#TODO: refactor this later,
# consider https://stackoverflow.com/questions/19073952/flask-restful-how-to-add-resource-and-pass-it-non-global-data
kafkaBrokers = "ec2-35-160-75-159.us-west-2.compute.amazonaws.com:9092,ec2-52-25-251-166.us-west-2.compute.amazonaws.com:9092,ec2-52-32-113-202.us-west-2.compute.amazonaws.com:9092"
kafkaConfig = {'bootstrap.servers': kafkaBrokers}
kafkaProducer = SegmentKafkaProducer(kafkaConfig)

class SegmentRESTProxyForKafka(Resource):
    """
        REST API Sink for segment webhook that publishes data to kafka
    """
    def get(self):
        return "this is an endpoint for segment"

    def post(self):
        print("RECIEVED POST REQUEST")

        kafkaProducer.produce(jsonObject)


if __name__ == '__main__':


    api.add_resource(SegmentRESTProxyForKafka, '/publishToKafka')
    app.run(debug = True)

