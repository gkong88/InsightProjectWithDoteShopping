import dash
import dash_core_components as dcc
import dash_html_components as html
import dash_daq as daq
import dash_table
import pandas as pd
import plotly.graph_objs as go
from dash.dependencies import Input, Output, State
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

current_scoring_fn_kwargs = get_latest_message('scores_config_running').value

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
    dcc.Graph(
        id='bar_graph2'
    ),
    html.H1(children='Control Center', style={'textAlign':'center', 'colors':colors['text']}),
    html.Div(
    [
        html.Label("Cold Max Score"),
        dcc.Input(id="cold_max_score", type="number", value=current_scoring_fn_kwargs['max_coldness_score']),
        html.Label("Cold Threshold for Previews"),
        dcc.Input(id="cold_threshold_previews", type="number", value=current_scoring_fn_kwargs['min_previews_threshold']),
        html.Label("Cold Threshold Steepness"),
        dcc.Input(id="cold_threshold_steepness", type="number", value=current_scoring_fn_kwargs['cold_threshold_steepness']),
        html.Label("Hot Max Score"),
        dcc.Input(id="hot_max_score", type="number", value=current_scoring_fn_kwargs['max_hotness_score']),
        html.Label("Hot Threshold for Click Thru Rate"),
        dcc.Input(id="hot_threshold_ctr", type="number", value=current_scoring_fn_kwargs['ctr_hotness_threshold']),
        html.Label("Hot THreshold Steepness"),
        dcc.Input(id="hot_threshold_steepness", type="number", value=current_scoring_fn_kwargs['hot_threshold_steepness']),
        html.Label("Total Score Offset"),
        dcc.Input(id="total_score_offset", type="number", value=current_scoring_fn_kwargs['score_offset']),
        html.Label("Enter Password"),
        dcc.Input(id="password", type="text", value="Enter Password"),
        html.Button(id='submit-button', n_clicks=0, children='Submit To Pipeline'),
        html.Div(id="output-state"),
    ]),
    html.H1(children='Monitoring', style={'textAlign':'center', 'colors':colors['text']}),
    html.H2(children='Heartbeat', style={'textAlign':'center', 'colors':colors['text']}),
    daq.Indicator(
        id='heartbeat-source'
    ),
    daq.Indicator(
        id='heartbeat-table-generator'
    ),
    daq.Indicator(
        id='heartbeat-sink'
    ),
    html.H2(children='Uptime Graph', style={'textAlign':'center', 'colors':colors['text']}),
    dcc.Graph(
        id='s3-uptime-graph'
    ),
    daq.StopButton(
      id='my-daq-stopbutton'
    ),
    dcc.Interval(
        id='interval-graph',
        interval=1 * 1000,  # in milliseconds
        n_intervals=0
    ),
    dcc.Interval(
        id='interval-heartbeat',
        interval=300 * 1000,  # in milliseconds
        n_intervals=0
    )
])


@app.callback([Output('bar_graph', 'figure'), Output('bar_graph2', 'figure')],
              [Input('interval-graph', 'n_intervals')])
def update_graph_live(n):
    message = get_latest_message(report_topic_name, report_config)
    df = pd.read_json(json.dumps(message.value), orient='index')
    df['date'] = [datetime.datetime.fromtimestamp(ts / 1000) for ts in df.POST_TIMESTAMP]
    max_ts = max(df.POST_TIMESTAMP)
    df['tsnorm'] = [(ts - max_ts) / 1000 / 60 / 60 for ts in df.POST_TIMESTAMP]
    figure1 = {
        'data': [{'x': df['tsnorm'], 'y': df['coldness_score'], 'type': 'bar', 'name': 'COLD score', 'width': 0.02,
                  'marker_color': colors['cold']},
                 {'x': df['tsnorm'], 'y': df['hotness_score'], 'type': 'bar', 'name': 'HOT score', 'width': 0.02,
                  'marker_color': colors['hot'],
                  'hovertext': ['Post ID: %s\nPreviews: %s\nFull Views: %s\nCTR: %s'
                               % (post_id, previews, full_views, full_views / max(previews, 1))
                               for post_id, previews, full_views in
                               zip(df['PROPERTIES_SHOPPABLE_POST_ID'], df['PREVIEW'], df['FULL_VIEW'])]}
    ],
        'layout': {'title': 'Post Scores. Last Updates: %s'%str(datetime.datetime.now().astimezone(timezone('US/Pacific'))),
                   'barmode': 'stack',
                   'xaxis': {'title': 'Hours Ago', 'range': [-2, 0]},
                   'yaxis': {'title': 'Score'}
                   }
    }

    figure2 = {
        'data': [{'x': df['tsnorm'], 'y': df['coldness_score'], 'type': 'bar', 'name': 'COLD score', 'width': 0.04,
                  'marker_color': colors['cold']},
                 {'x': df['tsnorm'], 'y': df['hotness_score'], 'type': 'bar', 'name': 'HOT score', 'width': 0.04,
                  'marker_color': colors['hot'],
                  'hovertext': ['Post ID: %s\nPreviews: %s\nFull Views: %s\nCTR: %s'
                                % (post_id, previews, full_views, full_views / max(previews, 1))
                                for post_id, previews, full_views in
                                zip(df['PROPERTIES_SHOPPABLE_POST_ID'], df['PREVIEW'], df['FULL_VIEW'])]}
                 ],
        'layout': {
            'title': 'Post Scores. Last Updates: %s' % str(datetime.datetime.now().astimezone(timezone('US/Pacific'))),
            'barmode': 'stack',
            'xaxis': {'title': 'Hours Ago', 'range': [-24, 0]},
            'yaxis': {'title': 'Score'}
            }
    }
    return figure1, figure2


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
    if time_since_heartbeat_s <= 5 * 60:
        # heard from within 5 minutes. all is well
        color = colors['alive']
        value = True
    else:
        # haven't heard from in 5 minutes. pronounced dead.
        color = colors['dead']
        value = False
    return label, color, value


@app.callback(Output('s3-uptime-graph', 'figure'),
              [Input('interval-heartbeat', 'n_intervals')])
def service_uptime(n):
    bootstrap_servers = ['ec2-100-20-18-195.us-west-2.compute.amazonaws.com:9092',
                         'ec2-100-20-8-59.us-west-2.compute.amazonaws.com:9092',
                         'ec2-100-20-75-14.us-west-2.compute.amazonaws.com:9092']

    c = KafkaConsumer(bootstrap_servers=bootstrap_servers,
                      value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                      auto_offset_reset='earliest',
                      enable_auto_commit=False)

    tp = TopicPartition('connector_s3_sink_push_log', 0)
    c.assign([tp])

    # get latest message
    offset = c.end_offsets([tp])[tp] - 1
    end_offset = c.end_offsets([tp])[tp] - 1
    # downtime is defined as no s3 push within last 5 minutes.
    push_interval_SLA_limit_ms = 1000 * 60 * 5
    last_timestamp_epoch_ms = 9999999999999

    records = []
    total_downtime_ms = 0
    ## check for downtime between pushes
    for m in c:
        if m is None or m.offset >= end_offset:
            # all data read
            break
        # does time between pushes exceed limit?
        if m.timestamp - last_timestamp_epoch_ms > push_interval_SLA_limit_ms:
            # add points on graph that mark service as down between these two points
            records.append({'ts': last_timestamp_epoch_ms + 1, 'up_flag': 0, 's3_filename': ''})
            records.append({'ts': m.timestamp - 1, 'up_flag': 0, 's3_filename': ''})
            total_downtime_ms += m.timestamp - last_timestamp_epoch_ms
        records.append({'ts': m.timestamp, 'up_flag': 1, 's3_filename': m.value['filename']})
        last_timestamp_epoch_ms = m.timestamp
    ## also check for downtime from last push to now
    now = round(time.time() * 1000)
    if now - last_timestamp_epoch_ms > last_timestamp_epoch_ms:
        records.append({'ts': last_timestamp_epoch_ms + 1, 'up_flag': 0, 's3_filename': ''})
        records.append({'ts': now, 'up_flag': 0, 's3_filename': ''})
        total_downtime_ms += m.timestamp - last_timestamp_epoch_ms
    else:
        records.append({'ts': now, 'up_flag': 1, 's3_filename': ''})

    # enrich with a datetime column
    df = pd.io.json.json_normalize(records)
    df['date'] = [datetime.datetime.fromtimestamp(ts / 1000) for ts in df.ts]

    total_time_ms = df['ts'].max() - df['ts'].min()
    service_uptime_rate_percent = (1 - total_downtime_ms / total_time_ms) * 100
    figure = {
            'data': [{'x': df['date'], 'y': df['up_flag'], 'text': df['s3_filename'], 'type': 'scatter'}],
            'layout': {
                'title': 'Uptime over last %s Hours: %s%%'%(round(total_time_ms/1000/60/60, 0), round(service_uptime_rate_percent, 3)),
                'xaxis': {'title': 'Date'},
                'yaxis': {'title': 'up'},
                'hovermode': 'closest'
            }
    }
    return figure


@app.callback(Output('output-state', 'children'),
              [Input('submit-button', 'n_clicks')],
              [State('cold_max_score', 'value'),
               State('cold_threshold_previews', 'value'),
               State('cold_threshold_steepness', 'value'),
               State('hot_max_score', 'value'),
               State('hot_threshold_ctr', 'value'),
               State('hot_threshold_steepness', 'value'),
               State('total_score_offset', 'value'),
               State('password', 'value')])
def update_output(n_clicks,
                  cold_max_score, cold_threshold_previews, cold_threshold_steepness,
                  hot_max_score, hot_threshold_ctr, hot_threshold_steepness,
                  total_score_offset, password):
    #if password != u'password':
    #    return u'Password is incorrect!'
    try:
        input_scoring_fn_kwargs = {}
        input_scoring_fn_kwargs['max_coldness_score'] = cold_max_score
        input_scoring_fn_kwargs['min_previews_threshold'] = cold_threshold_previews
        input_scoring_fn_kwargs['cold_threshold_steepness'] = cold_threshold_steepness
        input_scoring_fn_kwargs['max_hotness_score'] = hot_max_score
        input_scoring_fn_kwargs['ctr_hotness_threshold'] = hot_threshold_ctr
        input_scoring_fn_kwargs['hot_threshold_steepness'] = hot_threshold_steepness
        input_scoring_fn_kwargs['score_offset'] = total_score_offset
        p = KafkaProducer(bootstrap_servers=bootstrap_servers, value_serializer=lambda x: json.dumps(x).encode('utf-8'))
        p.send(topic='scores_config_update', value=input_scoring_fn_kwargs)
        #p.send(topic='scores_config_update', value={'max_coldness_score': cold_max_score})
        p.flush()
        p.close()
        return u'Config change accepted! Sent to pipeline.'
    except:
        return u'Inputs invalid. Please check inputs and try again'



    return u'''
        The Button has been pressed {} times,
        Input 1 is "{}",
        and Input 2 is "{}"
    '''.format(n_clicks, input1, input2)


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
