import json
import pdb
import confluent_kafka
import time
import random
from datetime import datetime
from dateutil.parser import parse

class EventGenerator():
    def __init__(self, csvPathString):
        self.csvPathString = csvPathString
        self.__readFile__(csvPathString)

    def __readFile__(self, csvPathString):
        self.fid = open(csvPathString)
        self.fid.readline() #discard header
        self.streamEventTimeStart = parse(self.fid.readline().split(',')[1])
        self.timerStart = datetime.now()
        self.count = 0
        
    """
    returns next shoppable post_id

    blocks until sufficient time has passed
    """
    def get(self):
        line = self.fid.readline()
        self.count += 1
        if len(line) == 0:
            self.__readFile__()
            line = self.fid.readline()
        row = line.split(',')
        key = row[0]
        return key

def sendHTTPmessage():
    pass


def main():
    generator = EventGenerator("snapshot_posts.csv")
    # load an example json
    example = json.loads(open("shoppable_fit_example.json").read())
    # service discovery
    broker = "ec2-35-160-75-159.us-west-2.compute.amazonaws.com:9092,ec2-52-25-251-166.us-west-2.compute.amazonaws.com:9092,ec2-52-32-113-202.us-west-2.compute.amazonaws.com:9092"      
    topic = "Viewed Shoppable Fit"
    # load kafka config details
    conf = {'bootstrap.servers': broker}
    # initialize a connection to kafka producer
    p = confluent_kafka.Producer(**conf)
    counter = 0
    while True:
        key = generator.get()
        # sendHTTPmessage(key, example)
        p.produce(topic, example, key)
        counter += 1
        if counter % 20 == 0:
            counter = 0
            time.sleep(1)

if __name__ == "__main__":
    main()