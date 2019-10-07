import dash
import dash_core_components as dcc
import dash_html_components as html
import dash_daq as daq
import dash_table
import pandas as pd
import plotly.graph_objs as go
from dash.dependencies import Input, Output
from kafka import KafkaConsumer, KafkaProducer, TopicPartition
import sys, os
sys.path.insert(0, os.path.abspath('../util'))
from utility import get_latest_message
import json
import time
import datetime
from pytz import timezone


external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
app = dash.Dash(__name__, external_stylesheets = external_stylesheets)
colors = {
    'background': '#111111',
    'text': '#7FDBFF',
    'cold': '#00ffff',
    'hot': '#ff3300',
    'dead': '#FF0000',
    'alive': '#00FF30'
}

bootstrap_servers = ['ec2-100-20-18-195.us-west-2.compute.amazonaws.com:9092',
                     'ec2-100-20-8-59.us-west-2.compute.amazonaws.com:9092',
                     'ec2-100-20-75-14.us-west-2.compute.amazonaws.com:9092']
report_config = {'bootstrap_servers': bootstrap_servers,
                  'auto_offset_reset': 'latest',
                  'enable_auto_commit': True,
                  'value_deserializer': lambda x: json.loads(x.decode('utf-8'))}
heartbeat_config = {'bootstrap_servers': bootstrap_servers,
                    'auto_offset_reset': 'latest', 
                    'enable_auto_commit': True}
report_topic_name = 'recent_posts_scores_snapshot'
heartbeat_topic_name_sink = 'heartbeat_conn_s3_sink'
heartbeat_topic_name_source = 'heartbeat_conn_segment_source'
heartbeat_topic_name_table_generator = 'heartbeat_table_generator'
s3_topic_name = 'connector_s3_sink_push_log'

message = get_latest_message(report_topic_name, report_config)
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
        id='bar_graph'
    ),
    dash_table.DataTable(
        id='top-n-table',
        columns=[{"name": i, "id": i} for i in df.columns],
    ),
    daq.Indicator(
        id='heartbeat-source'
    ),
    daq.Indicator(
        id='heartbeat-table-generator'
    ),
    daq.Indicator(
        id='heartbeat-sink'
    ),
    dcc.Graph(
        id='s3-push-history-graph'
    ),
    dcc.Interval(
        id='interval-graph',
        interval=1 * 1000,  # in milliseconds
        n_intervals=0
    ),
    dcc.Interval(
        id='interval-heartbeat',
        interval=60 * 1000,  # in milliseconds
        n_intervals=0
    )

])


@app.callback(Output('bar_graph', 'figure'),
              [Input('interval-graph', 'n_intervals')])
def update_graph_live(n):
    message = get_latest_message(report_topic_name, report_config)
    df = pd.read_json(json.dumps(message.value), orient='index')
    df['date'] = [datetime.datetime.fromtimestamp(ts / 1000) for ts in df.POST_TIMESTAMP]
    max_ts = max(df.POST_TIMESTAMP)
    df['tsnorm'] = [(ts - max_ts) / 1000 / 60 / 60 for ts in df.POST_TIMESTAMP]
    figure = {
        'data': [{'x': df['tsnorm'], 'y': df['coldness_score'], 'type': 'bar', 'name': 'COLD score', 'width': 0.025,
                  'marker_color': colors['cold']},
                 {'x': df['tsnorm'], 'y': df['hotness_score'], 'type': 'bar', 'name': 'HOT score', 'width': 0.025,
                  'marker_color': colors['hot'],
                  'hovertext': ['Post ID: %s\nPreviews: %s\nFull Views: %s\nCTR: %s'
                               % (post_id, previews, full_views, full_views / max(previews, 1))
                               for post_id, previews, full_views in
                               zip(df['PROPERTIES_SHOPPABLE_POST_ID'], df['PREVIEW'], df['FULL_VIEW'])]}
    ],
        'layout': {'title': 'Post Scores. Last Updates: %s'%str(datetime.datetime.now().astimezone(timezone('US/Pacific'))),
                   'barmode': 'stack',
                   'xaxis': {'title': 'Hours Ago', 'range': [-6, 0]},
                   'yaxis': {'title': 'Score'}
                   }
    }
    return figure

@app.callback(Output('top-n-table', 'data'),
              [Input('interval-graph', 'n_intervals')])
def update_table(n):
    #TODO: dry with update graph
    message = get_latest_message(report_topic_name, report_config)
    df = pd.read_json(json.dumps(message.value), orient='index')
    df = df.nlargest(20, 'score')
    df['date'] = [datetime.datetime.fromtimestamp(ts / 1000) for ts in df.POST_TIMESTAMP]
    max_ts = max(df.POST_TIMESTAMP)
    df['tsnorm'] = [(ts - max_ts) / 1000 / 60 / 60 for ts in df.POST_TIMESTAMP]
    return df.to_dict('records')

@app.callback([Output('heartbeat-source', 'label'), Output('heartbeat-source', 'color'), Output('heartbeat-source', 'value')],
              [Input('interval-heartbeat', 'n_intervals')])
def heartbeat_source(n):
    last_heartbeat_timestamp_s = get_latest_message(heartbeat_topic_name_source, heartbeat_config).timestamp / 1000
    last_heartbeat_date = datetime.datetime.fromtimestamp(last_heartbeat_timestamp_s, timezone('US/Pacific'))
    now_s = round(time.time())
    time_since_heartbeat_s = now_s - last_heartbeat_timestamp_s
    label = "Source Connector Status - Last Heartbeat Timestamp: %s - Minutes Ago: %s"%(str(last_heartbeat_date), str(round(time_since_heartbeat_s/60)))
    if time_since_heartbeat_s <= 5 * 60:
        # heard from within 5 minutes. all is well
        color = colors['alive']
        value = True
    else:
        # haven't heard from in 5 minutes. pronounced dead.
        color = colors['dead']
        value = False
    return label, color, value

@app.callback([Output('heartbeat-table-generator', 'label'), Output('heartbeat-table-generator', 'color'), Output('heartbeat-table-generator', 'value')],
              [Input('interval-heartbeat', 'n_intervals')])
def heartbeat_table_generator(n):
    #TODO: make dry with other heartbeats
    last_heartbeat_timestamp_s = get_latest_message(heartbeat_topic_name_table_generator, heartbeat_config).timestamp / 1000
    last_heartbeat_date = datetime.datetime.fromtimestamp(last_heartbeat_timestamp_s, timezone('US/Pacific'))
    now_s = round(time.time())
    time_since_heartbeat_s = now_s - last_heartbeat_timestamp_s
    label = "Table Generator Status - Last Heartbeat Timestamp: %s - Minutes Ago: %s"%(str(last_heartbeat_date), str(round(time_since_heartbeat_s/60)))
    if (time_since_heartbeat_s) <= 5 * 60:
        # heard from within 5 minutes. all is well
        color = colors['alive']
        value = True
    else:
        # haven't heard from in 5 minutes. pronounced dead.
        color = colors['dead']
        value = False
    return label, color, value

@app.callback([Output('heartbeat-sink', 'label'), Output('heartbeat-sink', 'color'), Output('heartbeat-sink', 'value')],
              [Input('interval-heartbeat', 'n_intervals')])
def heartbeat_sink(n):
    #TODO: make dry with other heartbeats
    last_heartbeat_timestamp_s = get_latest_message(heartbeat_topic_name_sink, heartbeat_config).timestamp / 1000
    last_heartbeat_date = datetime.datetime.fromtimestamp(last_heartbeat_timestamp_s, timezone('US/Pacific'))
    now_s = round(time.time())
    time_since_heartbeat_s = now_s - last_heartbeat_timestamp_s
    label = "S3 Sink Connector Status - Last Heartbeat Timestamp: %s - Minutes Ago: %s"%(str(last_heartbeat_date), str(round(time_since_heartbeat_s/60)))
    if (time_since_heartbeat_s) <= 5 * 60:
        # heard from within 5 minutes. all is well
        color = colors['alive']
        value = True
    else:
        # haven't heard from in 5 minutes. pronounced dead.
        color = colors['dead']
        value = False
    return label, color, value

@app.callback(Output('s3-push-history-graph', 'figure'),
              [Input('interval-heartbeat', 'n_intervals')])
def s3_push_history(n):
    message = get_latest_message(s3_topic_name, report_config)
    df = pd.read_json(json.dumps(message.value), orient='index')
    df['date'] = [datetime.datetime.fromtimestamp(ts / 1000) for ts in df.POST_TIMESTAMP]
    max_ts = max(df.POST_TIMESTAMP)
    df['tsnorm'] = [(ts - max_ts) / 1000 / 60 / 60 for ts in df.POST_TIMESTAMP]
    figure = {
        'data': [{'x': df['tsnorm'], 'y': df['hotness_score'], 'type': 'bar', 'name': 'COLD score', 'width': 0.025,
                  'marker_color': colors['hot'],
                  'hovertext': ['Post ID: %s\nPreviews: %s\nFull Views: %s\nCTR: %s'
                                % (post_id, previews, full_views, full_views / max(previews, 1))
                                for post_id, previews, full_views in
                                zip(df['PROPERTIES_SHOPPABLE_POST_ID'], df['PREVIEW'], df['FULL_VIEW'])]},
                 {'x': df['tsnorm'], 'y': df['coldness_score'], 'type': 'bar', 'name': 'HOT score', 'width': 0.025,
                  'marker_color': colors['cold']}],
        'layout': {
            'title': 'Post Scores. Last Updates: %s' % str(datetime.datetime.now().astimezone(timezone('US/Pacific'))),
            'barmode': 'stack',
            'xaxis': {'title': 'Hours Ago', 'range': [-6, 0]},
            'yaxis': {'title': 'Score'}
            }
    }
    return figure




if __name__ == '__main__':
    # main()
    app.run_server(host = '0.0.0.0', port = 8050, debug = True)


# @app.callback(
#     Output("hot-cold-bar", "value"),
#     [Input("hot-cold-slider", "value")],
# )
# def update_output(cold_value):
#     return cold_value

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
