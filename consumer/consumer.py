import json
from kafka import KafkaConsumer
import sys
import smart_open
import time
import datetime

def get_scores(msgs, end_offset):
    scores = {}
    counter = 0
    for m in msgs:
        if m != None:
            scores[m.value['PROPERTIES_SHOPPABLE_POST_ID']] = m.value['RT_SCORE']
            counter += 1
            if counter % 1000 == 0:
                print("message processing counter: %s, offset: %s" %(counter, m.offset))
        if m.offset >= end_offset:
            break
    return scores;

def push_to_s3(scores):
    push_timestamp = datetime.datetime.now()
    push_timestamp_str = push_timestamp.strftime('%Y%m%d%H%M%S')
    print("Pushing data to S3 at: %s"%push_timestamp_str)
    with smart_open.open('s3://dote-fit-scores/calculated_score_2/rtscore_' +  push_timestamp_str + '.csv', 'wb') as fout:
        fout.write(('shoppable_post_id,score\n').encode('utf-8'))
        for post, score in scores.items():
            fout.write((str(post) + ',' + str(int(score))+'\n').encode('utf-8'))
    print("Push successful!")
    return push_timestamp


def reset_consumer(c, time_window):
    p = c.assignment().pop()
    start_offset = c.offsets_for_times({p: int(time.time() * 1000) - time_window})[p].offset
    end_offset = c.end_offsets([p])[p]
    c.seek(p, start_offset)
    return end_offset


time_window = 3 * 24 * 60 * 60 * 1000
topic_name = 'CLICK__FI_RECENT_POST__AG_COUNTS__EN_SCORE2'
servers = 'ec2-100-20-18-195.us-west-2.compute.amazonaws.com:9092'
push_interval = datetime.timedelta(minutes = 2)

while True:
    c = KafkaConsumer(topic_name,                                                                                                                                                                                                                                                                     bootstrap_servers=servers,                                                                                                                                                                                                                                                      auto_offset_reset='earliest',                                                                                                                                                                                                                                                   enable_auto_commit=True,                                                                                                                                                                                                                                                        group_id='my-group',                                                                                                                                                                                                                                                            value_deserializer=lambda x: json.loads(x.decode('utf-8'))                                                                                                                                                                                                                      )                                                              
    m = c.poll(1,1) #poll kafka topic until consumer partition is automatically generated
    while len(m) == 0: m = c.poll(1,1)
    end_offset = reset_consumer(c, time_window)
    scores = get_scores(c, end_offset)
    last_push_timestamp = push_to_s3(scores)
    c.close()
    next_push_timestamp = last_push_timestamp + push_interval
    while (datetime.datetime.now() < next_push_timestamp):
        print("sleeping until %s"%next_push_timestamp)
        time.sleep((datetime.datetime.now() - (last_push_timestamp + push_interval)).seconds)

