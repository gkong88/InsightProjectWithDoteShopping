from gevent.pywsgi import WSGIServer
from flask import Flask, request
import json
from flask_restful import Resource, Api
import confluent_kafka
import flatten_json
import pdb
import datetime

# Initialize central registry for your app
app = Flask(__name__)
api = Api(app)

# Ref: https://stackoverflow.com/questions/6999726/
# How to convert time to epoch
epoch = datetime.datetime.utcfromtimestamp(0)
def segment_timestamp_to_unix_millis(segment_timestamp_str: str):
    """
    casts segment string timestamp to unix millis timestamp that Kafka supports

    Required: segment_timestamp_str should be in the following format
     "2019-09-21T20:31:07.942Z"
    """
    segment_timestamp_datetime = datetime.datetime.strptime(segment_timestamp_str, "%Y-%m-%dT%H:%M:%S.%fZ")
    return int((segment_timestamp_datetime - epoch).total_seconds() * 1000)

class SegmentKafkaProducer:
    """
    Takes segment events and creates Kafka events
    """
    def __init__(self, kafka_config: str, topic_json_key, key_json_key: str = None, key_timestamp: str = None,
                 validate: bool = True):
        self.topic_json_key = topic_json_key
        self.key_json_key = key_json_key
        self.key_timestamp = key_timestamp
        self.kafka_producer = confluent_kafka.Producer(**kafka_config)
        self.validate = validate

    def produce(self, request):
        """
        #TODO
        """
        pdb.set_trace()
        flattened_json_object = flatten_json.flatten(json.loads(request.data))
        #TODO a little bit sloppy here, consider a refactor
        if flattened_json_object["event"] in ["Viewed Shoppable Fit", "Created Story"]:
            topic = ''.join(c for c in str(flattened_json_object["type"] + flattened_json_object[self.topic_json_key]) if c.isalnum()) + "_00_raw_flatJSON"
            key = flattened_json_object.get(self.key_json_key)
            if self.key_timestamp is not None:
                timestamp = segment_timestamp_to_unix_millis(flattened_json_object.get(self.key_timestamp))
                self.kafka_producer.produce(topic, json.dumps(flattened_json_object), key, timestamp)
            else:
                self.kafka_producer.produce(topic, json.dumps(flattened_json_object), key)
        else: 
            self.kafka_producer.produce("other", json.dumps(flattened_json_object))
        





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
        #TODO: refactor this later,
        # consider https://stackoverflow.com/questions/19073952/flask-restful-how-to-add-resource-and-pass-it-non-global-data
        kafka_brokers_list = "ip-10-0-0-55.us-west-2.compute.internal:9092,ip-10-0-0-169.us-west-2.compute.internal:9092,ip-10-0-0-245.us-west-2.compute.internal:9092"
        kafka_config = {'bootstrap.servers': kafka_brokers_list}
        kafka_producer = SegmentKafkaProducer(kafka_config, "event", "properties_shoppable_post_id")
        kafka_producer.produce(request)


if __name__ == '__main__':
    api.add_resource(SegmentRESTProxyForKafka, '/publishToKafka')
    http_server = WSGIServer(('', 5000), app, log = None)
    http_server.serve_forever()

