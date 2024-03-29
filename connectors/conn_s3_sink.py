import smart_open
from kafka import KafkaConsumer, KafkaProducer, TopicPartition
import json
import datetime
from pytz import timezone
import time
import os, sys
sys.path.insert(0, os.path.abspath('../util'))
from utility import RepeatPeriodically, heartbeat, get_latest_message
from typing import Sequence


class S3SinkConnector:
    """
    Sink Connector to S3 from stream analysis.

    REQUIRES input topic to have only one partition for global ordering.
    """
    def __init__(self, input_topic_name: str, bootstrap_servers: Sequence[str],
                 s3_bucket_path: str,
                 log_topic_name: str,
                 min_push_interval: datetime.timedelta):
        # cast sequence to list, if not already list. precondition of KafkaConsumer
        self.input_topic_name = input_topic_name
        bootstrap_servers = list(bootstrap_servers)
        self.consumer_config = {'bootstrap_servers': bootstrap_servers,
                                'auto_offset_reset': 'latest',
                                'enable_auto_commit': True,
                                'value_deserializer': lambda x: json.loads(x.decode('utf-8'))}
        self.producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                                      value_serializer=lambda x: json.dumps(x).encode('utf-8'))
        self.log_topic_name = log_topic_name

        self.s3_bucket_path = s3_bucket_path
        self.min_push_interval = min_push_interval

    def run_forever(self):
        while True:
            # poll kafka topic until consumer partition is automatically generated
            message = get_latest_message(self.input_topic_name, self.consumer_config)
            if message is None:
                continue

            # push latest scores to s3, log this push
            last_push_filename, last_push_timestamp = self.__push_to_s3(message)
            self.__log_push_to_s3(last_push_filename)

            # wait until the next push interval
            next_push_timestamp = last_push_timestamp + self.min_push_interval
            while datetime.datetime.now() < next_push_timestamp:
                sleep_duration = max((next_push_timestamp - datetime.datetime.now()).seconds, 1)
                print("sleeping until %s" % next_push_timestamp)
                print("sleeping for %s seconds" % sleep_duration)
                time.sleep(sleep_duration)

    def __push_to_s3(self, message) -> datetime.datetime:
        """
        Push scores to s3 bucket.
        """
        # Assumes credentials are located in:
        #   ~/.aws/credentials
        scores = message.value

        push_timestamp = datetime.datetime.now()
        push_timestamp_pst_strf = push_timestamp.astimezone(timezone('US/Pacific')).strftime('%Y%m%d%H%M%S')

        print("Pushing data to S3 at: %s" % push_timestamp_pst_strf)
        s3_filename = self.s3_bucket_path + 'rtscore_' + push_timestamp_pst_strf + '.csv'
        with smart_open.open(s3_filename, 'wb') as fout:
            fout.write(b'shoppable_post_id,score\n')
            for post, values in scores.items():
                fout.write((str(post) + ',' + str(round(values['score'])) + '\n').encode('utf-8'))
        print("Push successful!")
        return s3_filename, push_timestamp

    def __log_push_to_s3(self, filename):
        self.producer.send(topic=self.log_topic_name, value={'filename': filename})


if __name__ == "__main__":
    input_topic_name = 'recent_posts_scores_snapshot_prod'
    bootstrap_servers = ['ec2-100-20-18-195.us-west-2.compute.amazonaws.com:9092',
                         'ec2-100-20-8-59.us-west-2.compute.amazonaws.com:9092',
                         'ec2-100-20-75-14.us-west-2.compute.amazonaws.com:9092']
    s3_bucket_path = 's3://dote-fit-scores/calculated_score_2/'
    log_topic_name = 'connector_s3_sink_push_log'
    min_push_interval = datetime.timedelta(minutes=2)

    heartbeat_kwargs = {'bootstrap_servers': bootstrap_servers, 'topic_name': 'heartbeat_conn_s3_sink'}
    RepeatPeriodically(fn=heartbeat, interval=120, kwargs=heartbeat_kwargs).run()

    sink = S3SinkConnector(input_topic_name = input_topic_name,
                           bootstrap_servers = bootstrap_servers,
                           s3_bucket_path = s3_bucket_path,
                           log_topic_name = log_topic_name,
                           min_push_interval = min_push_interval)
    sink.run_forever()

