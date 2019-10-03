import dash
import dash_core_components as dcc
import dash_html_components as html
import dash_daq as daq
import dash_table
import pandas as pd
import plotly.graph_objs as go
from dash.dependencies import Input, Output
from kafka import KafkaConsumer
import smart_open
import json
from scoring_function_creator import ScoringFunctionCreator
from recent_posts_table import RecentPostsTable
import time
import datetime
from pytz import timezone

def start_posts_table():
    # config variables
    topic_name = 'CLICK__FI_RECENT_POST__AG_COUNTS__EN_SCORE2'
    servers = 'ec2-100-20-18-195.us-west-2.compute.amazonaws.com:9092'
    # push_interval = datetime.timedelta(minutes=2)
    # connect to Kafka Topic.
    consumer = KafkaConsumer(topic_name,
                             bootstrap_servers=servers,
                             auto_offset_reset='earliest',
                             enable_auto_commit=True,
                             group_id='my-group',
                             value_deserializer=lambda x: json.loads(x.decode('utf-8')))
    scoring_function = ScoringFunctionCreator()
    return RecentPostsTable(consumer, scoring_function)

def update_posts():
    pass

if __name__ == "__main__":
    start_posts_table()


