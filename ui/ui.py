import dash
import dash_core_components as dcc
import dash_html_components as html
import dash_daq as daq
import dash_table
import pandas as pd
import plotly.graph_objs as go
from dash.dependencies import Input, Output
from kafka import KafkaConsumer, KafkaProducer, TopicPartition
import json
import time
import datetime
from pytz import timezone


def get_latest_message(input_topic_name: str):
    """

    :param input_topic_name:
        REQUIRES only one partition for global ordering
    :return:
    """
    # create consumer for topic
    consumer = KafkaConsumer(**default_config)
    partition_number = list(consumer.partitions_for_topic(input_topic_name))[0]
    topic_partition = TopicPartition(input_topic_name, partition_number)
    consumer.assign([topic_partition])

    # get latest message
    consumer.seek(topic_partition, consumer.end_offsets([topic_partition])[topic_partition] - 1)
    message = consumer.poll(3000, 1)[topic_partition][0]

    # close connection and return result
    consumer.close()
    return message

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
app = dash.Dash(__name__, external_stylesheets = external_stylesheets)
colors = {
    'background': '#111111',
    'text': '#7FDBFF',
    'cold': '#00ffff',
    'hot:': '#ff3300'
}

bootstrap_servers = ['ec2-100-20-18-195.us-west-2.compute.amazonaws.com:9092',
                     'ec2-100-20-8-59.us-west-2.compute.amazonaws.com:9092',
                     'ec2-100-20-75-14.us-west-2.compute.amazonaws.com:9092']
default_config = {'bootstrap_servers': bootstrap_servers,
                  'auto_offset_reset': 'latest',
                  'enable_auto_commit': True,
                  'value_deserializer': lambda x: json.loads(x.decode('utf-8'))}
report_topic_name = 'recent_posts_scores_snapshot'
log_topic_name = 'pipeline_logs'

message = get_latest_message(report_topic_name)
df = pd.read_json(json.dumps(message.value), orient='index')
df['date'] = [datetime.datetime.fromtimestamp(ts / 1000) for ts in df.POST_TIMESTAMP]
max_ts = max(df.POST_TIMESTAMP)
df['tsnorm'] = [(ts - max_ts) / 1000 / 60 / 60 for ts in df.POST_TIMESTAMP]

app.layout = html.Div([
    html.H1(
        children='Real-Time Scoring Dashboard',
        style={
            'textAlign': 'center',
            'colors': colors['text']
        }
    ),
    dcc.Graph(
        id='my-graph',
        figure={
            'data': [{'x': df['tsnorm'], 'y': df['hotness_score'], 'type': 'bar', 'name': 'cold', 'width': 0.05, 'marker_color': colors['hot']},
                     {'x': df['tsnorm'], 'y': df['coldness_score'], 'type': 'bar', 'name': 'cold', 'width': 0.05, 'marker_color': colors['cold']}],
            'layout': {'title': 'Post Scores',
                       'barmode': 'stack',
                       'xaxis': {'title': 'Hours Ago', 'range': [-3, 0]},
                       'yaxis': {'title': 'Score'}
                       }
        }
    )
# ,
    # html.Div(
    #     [
    #     # html.Label('Exploit vs Explore'),
    #     html.H2(
    #         children = "CONTROL CENTER",
    #         style = {
    #             'textAlign': 'center',
    #             'colors': colors['text']
    #         }
    #     ),
    #     dcc.Slider(
    #         id='hot-cold-slider',
    #         min=0,
    #         max=100,
    #         marks={i: 'Label {}'.format(i) if i == 1 else str(i) for i in range(0, 110, 10)},
    #         value=50
    #     ),
    #     # daq.GraduatedBar(
    #     #     id='hot-cold-bar',
    #     #     color={"ranges":{"blue":[0,4],"red":[7,10]}},
    #     #     showCurrentValue=True,
    #     #     value=10
    #     # )
    #     dash_table.DataTable(
    #         id='my-table'
    #         #columns=[{"name": i, "id": i} for i in df.columns],
    #         #data=backup_df.to_dict('records')
    #     ),
    #     dcc.Interval(
    #         id='interval-component',
    #         interval=1 * 1000,  # in milliseconds
    #         n_intervals=0
    #     )
    #     ]
    # )
])

# @app.callback(
#     Output("hot-cold-bar", "value"),
#     [Input("hot-cold-slider", "value")],
# )
# def update_output(cold_value):
#     return cold_value


# Multiple components can update everytime interval gets fired.
# @app.callback(Output('my-graph', 'figure'),
#               [Input('interval-component', 'n_intervals')])
# def update_graph_live(n):
#     message = get_latest_message(report_topic_name)
#     df = pd.read_json(json.dumps(message.value), orient='index')
#     df['date'] = [datetime.datetime.fromtimestamp(ts / 1000) for ts in df.POST_TIMESTAMP]
#     max_ts = max(df.POST_TIMESTAMP)
#     df['tsnorm'] = [(ts - max_ts) / 1000 / 60 / 60 for ts in df.POST_TIMESTAMP]
#     figure = {
#         'data': [go.Bar(name='cold score',x=df['tsnorm'],y=df['coldness_score'],width=0.1,colors=colors['cold']),
#                 go.Bar(name='hot score', x=df['tsnorm'], y=df['hotness_score'], colors=colors['hot'], wid=0.1)],
#         'layout': go.Layout(
#             title='Post Scores in Past Three Days',
#             barmode='stack',
#             xaxis=dict(title='Hours Ago', range=[-3, 0]),
#             yaxis=dict(title='Score')
#         )}
#     return figure

if __name__ == '__main__':
    # main()
    app.run_server(host = '0.0.0.0', port = 8050, debug = True)

