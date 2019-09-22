import json
import pdb
import confluent_kafka
import time
import random

# load an example json
example = json.loads(open("shoppable_fit_example.json").read())

# service discovery
broker = "ec2-35-160-75-159.us-west-2.compute.amazonaws.com:9092,ec2-52-25-251-166.us-west-2.compute.amazonaws.com:9092,ec2-52-32-113-202.us-west-2.compute.amazonaws.com:9092"      
topic = "Viewed Shoppable Fit"

# load kafka config details
conf = {'bootstrap.servers': broker}

# initialize a connection to kafka producer
p = Producer(**conf)

while True:
    time.sleep(0.1)
    p.producer(topic, example, random.randint(100))


pdb.set_trace()