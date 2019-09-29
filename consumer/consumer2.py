import json
from kafka import KafkaConsumer
import sys
import smart_open
import time
import datetime
import pdb

def get_scores(msgs):
    scores = {}
    counter = 0
    for m in msgs:
        if m != None:
            scores[m.value['PROPERTIES_SHOPPABLE_POST_ID']] = m.value['RT_SCORE']
            counter += 1
            if counter % 1000 == 0:
                print("event processing counter: %s" % counter)
    return scores;

def push_to_s3(scores):
    with smart_open.smart_open('s3://dote-fit-scores/calculated_score_2/rtscore_' +  datetime.datetime.now().strftime('%Y%m%d%H%M%S') + '_.csv', 'wb') as fout:
        fout.write('shoppable_post_id,score\n')
        for post, score in scores.items():
            fout.write(str(post) + ',' + str(int(score)))

def reset_consumer(c, time_window):
    pdb.set_trace()
    p = c.assignment().pop()
    start_offset = c.offsets_for_times({p: int(time.time() * 1000) - time_window})[p].offset
    end_offset = c.end_offsets([p])[p]
    c.seek(p, start_offset)
    return end_offset


time_window = 3 * 24 * 60 * 60 * 1000
topic_name = 'CLICK__FI_RECENT_POST__AG_COUNTS__EN_SCORE2'
servers = 'ec2-100-20-18-195.us-west-2.compute.amazonaws.com:9092'

c = KafkaConsumer(topic_name,
                  bootstrap_servers=servers,
                  auto_offset_reset='earliest',
                  enable_auto_commit=True,
                  group_id='my-group',
                  value_deserializer=lambda x: json.loads(x.decode('utf-8'))
                  )
m = c.poll(1,1)
while len(m) == 0: m = c.poll(1,1)
while True:
    reset_consumer(c, time_window)
    scores = get_scores(c)
    push_to_s3(scores)
