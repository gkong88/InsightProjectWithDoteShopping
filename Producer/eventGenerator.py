import json
import confluent_kafka
import time
import flatten_json
import pdb

class EventGenerator():
    def __init__(self, csvPathString):
        self.csvPathString = csvPathString
        self.__readFile__()

    def __readFile__(self):
        self.fid = open(self.csvPathString)
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
        row = line.strip().split(',')
        return row[0], row[1]

def sendHTTPmessage():
    pass

def write_schema_to_file(flatJSON, topic):
    """
    Writes flatJSON to a file.

    Useful when selecting attributes in KSQL
    """
    fid = open(topic + ".json", 'w')
    fid.write(json.dumps(flatJSON))
    fid.close()


def main():
    generator = EventGenerator("snapshot.csv")
    # load an example json
    example = open("shoppable_fit_example.json",'r').read()
    flatJSON = flatten_json.flatten(json.loads(open("shoppable_fit_example.json",'r').read()))
    broker = "ec2-35-160-75-159.us-west-2.compute.amazonaws.com:9092,ec2-52-25-251-166.us-west-2.compute.amazonaws.com:9092,ec2-52-32-113-202.us-west-2.compute.amazonaws.com:9092"
    topic = ''.join(c for c in str(flatJSON['event'])if c.isalnum()) + "_00_raw_flatJSON"
    write_schema_to_file(flatJSON, topic)
    # load kafka config details
    conf = {'bootstrap.servers': "ec2-35-160-75-159.us-west-2.compute.amazonaws.com:9092"}
    # initialize a connection to kafka producer
    p = confluent_kafka.Producer(**conf)
    counter = 0
    while True:
        key, view = generator.get()
        flatJSON["properties_shoppable_post_id"]= str(key)
        flatJSON["properties_display"] = str(view)
        if view == 'full view':
            flatJSON["isfullview"] = 1
        else:
            flatJSON["isfullview"] = 0
            
        p.produce(topic, json.dumps(flatJSON), str(key))
        p.poll(0)
        counter += 1
        if counter % 10 == 0:
            counter = 0
            time.sleep(1)

if __name__ == "__main__":
    main()
