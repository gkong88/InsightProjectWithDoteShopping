from gevent.pywsgi import WSGIServer
from flask import Flask, request
import json
from flask_restful import Resource, Api
import confluent_kafka
import flatten_json
import pdb
import datetime
import requests

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

class SegmentRESTProxyForKafka(Resource):
    """
        REST API Sink for segment webhook that publishes data to kafka
    """
    def get(self):
        return "this is an endpoint for segment"

    def get_key(self, json_object):
        if str(json_object["event"]) in ["Viewed Shoppable Fit", "Created Story"]:
            return int(json_object["properties_shoppable_post_id"])
        else:
            return -1
 

    def post(self):
        """
        creates kafka event from json object

        :return: none
        """
        #TODO: refactor this later,
        # consider https://stackoverflow.com/questions/19073952/flask-restful-how-to-add-resource-and-pass-it-non-global-data
        flat_json_object = flatten_json.flatten(json.loads(request.data))
        segment_timestamp = segment_timestamp_to_unix_millis(flat_json_object.get("timestamp"))
        flat_json_object["segment_timestamp"] = segment_timestamp
        key = self.get_key(flat_json_object)
        kafka_payload_data = {"records": [{"value": flat_json_object, "key": key}]}
        topic = ''.join(c for c in str(flat_json_object["type"] + flat_json_object["event"]) if
                        c.isalnum()) + "_00_raw_flatJSON"
        destination_url = "http://ec2-52-36-231-83.us-west-2.compute.amazonaws.com:8082/topics/" + topic
        headers = {"Content-Type": "application/vnd.kafka.json.v2+json", "Accept": "application/vnd.kafka.v2+json"}
        response = requests.post(destination_url, json=kafka_payload_data, headers=headers)
        return response.text


if __name__ == '__main__':
    api.add_resource(SegmentRESTProxyForKafka, '/publishToKafka')
    http_server = WSGIServer(('', 5000), app, log = None)
    http_server.serve_forever()

