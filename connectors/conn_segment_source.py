from gevent.pywsgi import WSGIServer
from flask import Flask, request
import json
from flask_restful import Resource, Api
import flatten_json
import datetime
import requests
import os, sys
import resource
from kafka import KafkaProducer
sys.path.insert(0, os.path.abspath('../util'))
from utility import RepeatPeriodically, heartbeat


# Initialize central registry for your app
application = Flask(__name__)
api = Api(application)
bootstrap_servers = ['ec2-100-20-18-195.us-west-2.compute.amazonaws.com:9092', 'ec2-100-20-8-59.us-west-2.compute.amazonaws.com:9092', 'ec2-100-20-75-14.us-west-2.compute.amazonaws.com:9092']

# open up connection to kafka cluster
p = KafkaProducer(bootstrap_servers=bootstrap_servers,
                  value_serializer=lambda x: json.dumps(x).encode('utf-8'))

# spinoff thread that sends heartbeats
heartbeat_kwargs = {'bootstrap_servers': bootstrap_servers, 'topic_name': 'heartbeat_conn_segment_source'}
RepeatPeriodically(fn=heartbeat, interval=120, kwargs=heartbeat_kwargs).run()

@application.route("/")
def hello():
    return "<h1 style='color:blue'>This server is a connector from Segment to Kafka!</h1>" 


def segment_timestamp_to_unix_millis(segment_timestamp_str: str):
    """
    casts segment string timestamp to unix millis timestamp that Kafka supports
    Required: segment_timestamp_str should be in the following format
     "2019-09-21T20:31:07.942Z"
    """
    epoch = datetime.datetime.utcfromtimestamp(0)
    segment_timestamp_datetime = datetime.datetime.strptime(segment_timestamp_str, "%Y-%m-%dT%H:%M:%S.%fZ")
    return int((segment_timestamp_datetime - epoch).total_seconds() * 1000)


class SegmentSourceConnector(Resource):
    """
        REST API Sink for segment webhook that publishes data to kafka
    """
    def __init__(self):
        bootstrap_servers = ['ec2-100-20-18-195.us-west-2.compute.amazonaws.com:9092',
                             'ec2-100-20-8-59.us-west-2.compute.amazonaws.com:9092',
                             'ec2-100-20-75-14.us-west-2.compute.amazonaws.com:9092']
        # open up connection to kafka cluster
        self.p = KafkaProducer(bootstrap_servers=bootstrap_servers,
#                          key_serializer=lambda x: x.to_bytes(8, 'big'),
                          value_serializer=lambda x: json.dumps(x).encode('utf-8'))

    def get(self):
        return "this is an endpoint for segment"

    def get_key(self, json_object):
        """
        Extracts a key for a given event type, if applicable.

        Otherwise, returns a single key value.
        """
        # keys determine kafka topic partitions.
        # when performing stream analysis w/ joins, keys must be used on the join attribute.
        if str(json_object.get("event")) in ["Viewed Shoppable Fit", "Created Story"]:
            return int(json_object["properties_shoppable_post_id"])
        elif str(json_object.get("event")) in ['Assigned AB Test Shard']:
            return json_object["userId"]
        else:
            return 0

    def post(self):
        """
        Creates kafka event from json object

        :return: response code of post request to Kafka REST Proxy
        """
        # flatten json so nested attributes can be used in KSQL analysis
        flat_json_object = flatten_json.flatten(json.loads(request.data))

        # extract timestamp and add it to json
        segment_timestamp = segment_timestamp_to_unix_millis(flat_json_object.get("timestamp"))
        flat_json_object["segment_timestamp"] = segment_timestamp

        # determine topic to send event
        topic = ''.join(c for c in str(flat_json_object.get("type") + flat_json_object.get("event")) if
                        c.isalnum()) + "_00_raw_flatJSON"

        # extract key, if there is a registered attribute that should serve as key for this event
        key = self.get_key(flat_json_object)
        self.p.send(topic = topic, key = str(key).encode('utf-8'), value = flat_json_object)
        return 200


api.add_resource(SegmentSourceConnector, '/publishToKafka')
resource.setrlimit(resource.RLIMIT_NOFILE, (999999, 999999))

if __name__ == '__main__':
    application.run(host='0.0.0.0', port = 5000)

