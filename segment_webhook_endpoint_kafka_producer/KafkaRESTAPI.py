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

    def produce(self, jsonObject):
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
        kafkaProducer.produce(request.data)


if __name__ == '__main__':
    api.add_resource(SegmentRESTProxyForKafka, '/publishToKafka')
    app.run(debug = True)

