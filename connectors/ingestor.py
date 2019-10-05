from gevent.pywsgi import WSGIServer
from flask import Flask, request
import json
from flask_restful import Resource, Api
import flatten_json
import datetime
import requests

# Initialize central registry for your app
application = Flask(__name__)
api = Api(application)


@application.route("/")
def hello():
    print("got the request")
    return "<h1 style='color:blue'>Hello There!</h1>" 

def segment_timestamp_to_unix_millis(segment_timestamp_str: str):
    """
    casts segment string timestamp to unix millis timestamp that Kafka supports
    Required: segment_timestamp_str should be in the following format
     "2019-09-21T20:31:07.942Z"
    """
    epoch = datetime.datetime.utcfromtimestamp(0)
    segment_timestamp_datetime = datetime.datetime.strptime(segment_timestamp_str, "%Y-%m-%dT%H:%M:%S.%fZ")
    return int((segment_timestamp_datetime - epoch).total_seconds() * 1000)

class SegmentRESTProxyForKafka(Resource):
    """
        REST API Sink for segment webhook that publishes data to kafka
    """
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
            return int(json_object["user_id"])
        else:
            return -1

    def post(self):
        """
        creates kafka event from json object

        :return: none
        """
        # flatten json so nested attributes can be used in KSQL analysis
        flat_json_object = flatten_json.flatten(json.loads(request.data))
        # extract timestamp and add it to json
        segment_timestamp = segment_timestamp_to_unix_millis(flat_json_object.get("timestamp"))
        flat_json_object["segment_timestamp"] = segment_timestamp
        # extract key to mark kafka event
        key = self.get_key(flat_json_object)
        # package data in Kafka REST API format
        kafka_payload_data = {"records": [{"value": flat_json_object, "key": key}]}
        # create topic name based on event attribute of incoming data
        topic = ''.join(c for c in str(flat_json_object.get("type") + flat_json_object.get("event")) if
                        c.isalnum()) + "_00_raw_flatJSON"
        # encode topic name that event should goto as a URI on top of URL
        destination_url = "http://ec2-52-36-231-83.us-west-2.compute.amazonaws.com:8082/topics/" + topic
        headers = {"Content-Type": "application/vnd.kafka.json.v2+json", "Accept": "application/vnd.kafka.v2+json", "Connection":'close'}
        response = requests.post(destination_url, json=kafka_payload_data, headers=headers)
        return response.text


api.add_resource(SegmentRESTProxyForKafka, '/publishToKafka')

if __name__ == '__main__':
    application.run(host='0.0.0.0', port = 5000)

