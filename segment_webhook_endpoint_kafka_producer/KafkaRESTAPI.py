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
        flat_json_object = flatten_json.flatten(json.loads(request.data))
        topic = ''.join(c for c in str(flat_json_object["type"] + flat_json_object["event"]) if
                        c.isalnum()) + "_00_raw_flatJSON"
        kafka_payload_data = {"records": [{"value": flat_json_object}]}
        headers = {"Content-Type": "application/vnd.kafka.json.v2+json", "Accept": "application/vnd.kafka.v2+json"}
        # kafka_json_payload = json.dump({"records": [{"value": flat_json_object}]})
        destination_url = "http://ec2-52-36-231-83.us-west-2.compute.amazonaws.com:8082/topics/" + topic
        # destination_url = "http://ec2-52-36-231-83.us-west-2.compute.amazonaws.com:8082/topics/jsontest3"
        # requests.request(method = "POST",
                         # destination_url = destination_url,
                         # headers = headers)
        # response = requests.post(destination_url, json={"records":[{"value":{"foo":"bar"}}]}, headers=headers)
        response = requests.post(destination_url, json=kafka_payload_data, headers=headers)
        return response.text


if __name__ == '__main__':
    api.add_resource(SegmentRESTProxyForKafka, '/publishToKafka')
    http_server = WSGIServer(('', 5000), app, log = None)
    http_server.serve_forever()

