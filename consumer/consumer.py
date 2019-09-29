import json
from kafka import KafkaConsumer
import smart_open
import time
import datetime

"""
This program periodically event sources a Kafka Topic 
to reconstitute a snapshot of scores of recent posts (by post creation date)
by replaying the events.

It pushes the snapshot as csv to s3.

Note: Kafka topics have weak guarantees on the liveness of log compaction.
      An event with key = post, value = score that has been superceded
      by an update, may still live in the event log 
"""


def get_scores(consumer: KafkaConsumer, time_window_size: datetime.timedelta) -> dict:
    """
    Reads kafka topic as an event source to reconstitute a "snapshot" of
    scores for all posts by replaying them into a dictionary.

    Returns latest scores of posts within the time window parameter from current time.

    :param consumer: kafka consumer that subscribes to relevant topic.
        REQUIRED to have exactly one partition
    :param time_window_size: post creation time window to include in analysis, relative to current time
    :return: a dict with (post_id, score) pairs
    """
    time_window_start = datetime.datetime.now() - time_window_size
    time_window_start_epoch = int(time_window_start.timestamp() * 1000)
    consumer_seek_to_window_start(consumer, time_window_start)
    end_offset = consumer_get_latest_offset(consumer)

    scores = {}
    counter = 0
    for m in consumer:
        if m is not None and m.value['POST_TIMESTAMP'] > time_window_start_epoch:
            scores[m.value['PROPERTIES_SHOPPABLE_POST_ID']] = m.value['RT_SCORE']
            counter += 1
            if counter % 1000 == 0:
                print("message processing counter: %s, offset: %s" %(counter, m.offset))
        if m.offset >= end_offset:
            break
    return scores;


def push_to_s3(scores) -> datetime.datetime:
    """
    Push scores to s3 bucket.

    :param scores:
    :return:
    """
    # Assumes credentials are ASSUMED to be located in:
    #   ~/.aws/credentials
    push_timestamp = datetime.datetime.now()
    push_timestamp_str = push_timestamp.strftime('%Y%m%d%H%M%S')

    print("Pushing data to S3 at: %s" % push_timestamp_str)
    with smart_open.open('s3://dote-fit-scores/calculated_score_2/rtscore_' + push_timestamp_str + '.csv', 'wb') as fout:
        fout.write(('shoppable_post_id,score\n').encode('utf-8'))
        for post, score in scores.items():
            fout.write((str(post) + ',' + str(int(score))+'\n').encode('utf-8'))
    print("Push successful!")

    return push_timestamp


def consumer_seek_to_window_start(consumer: KafkaConsumer, time_window_start: datetime.datetime):
    """
    This function mutates the consumer to "seek" the kafka topic offset to that of the earliest event that
    is inside the time_window.

    :param consumer:
    :param time_window_start: time window of analysis. i.e. how old of posts we should consider
    :effects: seek to first event in time window
    """
    topic_partition = consumer.assignment().pop()
    time_window_start_epoch = int(time_window_start.timestamp()*1000)

    # get first offset that is in the time window
    start_offset = consumer.offsets_for_times({topic_partition: time_window_start_epoch})[topic_partition].offset
    # set the consumer to consume from this offset
    consumer.seek(topic_partition, start_offset)


def consumer_get_latest_offset(consumer: KafkaConsumer) -> int:
    """
    Returns latest offset in topic of kafka consumer

    :param consumer: kafka consumer
    :return: kafka topic offset of last event within the time window of analysis
    """
    topic_partition = consumer.assignment().pop()
    end_offset = consumer.end_offsets([topic_partition])[topic_partition]
    return end_offset


if __name__ == "__main__":
    # config variables
    # TODO: refactor to take these in commandline
    time_window_size = datetime.timedelta(days=3)
    topic_name = 'CLICK__FI_RECENT_POST__AG_COUNTS__EN_SCORE2'
    servers = 'ec2-100-20-18-195.us-west-2.compute.amazonaws.com:9092'
    push_interval = datetime.timedelta(minutes=2)

    while True:
        # connect to Kafka Topic.
        consumer = KafkaConsumer(topic_name, bootstrap_servers=servers, auto_offset_reset='earliest', enable_auto_commit=True, group_id='my-group', value_deserializer=lambda x: json.loads(x.decode('utf-8')))

        # poll kafka topic until consumer partition is automatically generated
        message = consumer.poll(1, 1)
        while len(message) == 0:
            message = consumer.poll(1, 1)

        # get scores via event sourcing
        scores = get_scores(consumer, time_window_size)
        last_push_timestamp = push_to_s3(scores)
        consumer.close()

        # wait until the next push interval.
        next_push_timestamp = last_push_timestamp + push_interval
        while (datetime.datetime.now() < next_push_timestamp):
            print("sleeping until %s"%next_push_timestamp)
            time.sleep((datetime.datetime.now() - (last_push_timestamp + push_interval)).seconds)

