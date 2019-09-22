import json
import confluent_kafka
import time

class EventGenerator():
    def __init__(self, csvPathString):
        self.csvPathString = csvPathString
        self.__readFile__(csvPathString)

    def __readFile__(self, csvPathString):
        self.fid = open(csvPathString)
        self.fid.readline() #discard header
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
    generator = EventGenerator("snapshot.csv")
    # load an example json
    example = open("shoppable_fit_example.json").read()
    exampleJson = json.loads(open("shoppable_fit_example.json").read())
    # service discovery
    broker = "ec2-35-160-75-159.us-west-2.compute.amazonaws.com:9092,ec2-52-25-251-166.us-west-2.compute.amazonaws.com:9092,ec2-52-32-113-202.us-west-2.compute.amazonaws.com:9092"      
    topic = "ViewedShoppableFit2"
    # load kafka config details
    conf = {'bootstrap.servers': "ec2-35-160-75-159.us-west-2.compute.amazonaws.com:9092"}
    # initialize a connection to kafka producer
    p = confluent_kafka.Producer(**conf)
    counter = 0
    while True:
        key = generator.get()
        # sendHTTPmessage(key, example)
        p.produce(topic, "val", key)
        counter += 1
        if counter % 50 == 0:
            print("50 messages sent")
            counter = 0
            time.sleep(1)

if __name__ == "__main__":
    main()
