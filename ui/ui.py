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


topic_name = 'recent_posts_scores_snapshot'
kafka_servers = kafka_servers = 'ec2-100-20-18-195.us-west-2.compute.amazonaws.com:9092,ec2-100-20-8-59.us-west-2.compute.amazonaws.com:9092,ec2-100-20-75-14.us-west-2.compute.amazonaws.com:9092'
consumer = KafkaConsumer(topic_name,
                         bootstrap_servers=kafka_servers,
                         auto_offset_reset='latest',
                         enable_auto_commit=True,
                         group_id='my-group',
                         value_deserializer=lambda x: json.loads(x.decode('utf-8')))
if len(consumer.assignment()) == 0:
    # poll consumer to generate a topic partition assignment
    message = consumer.poll(1, 1)
    while len(message) == 0:
        message = consumer.poll(1, 1)
topic_partition = consumer.assignment().pop()
last_offset = -1

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
app = dash.Dash(__name__, external_stylesheets = external_stylesheets)
colors = {
    'background': '#111111',
    'text': '#7FDBFF',
    'cold': '#00ffff',
    'hot:': '#ff3300'
}

app.layout = html.Div([
    html.H1(
        children = 'Real-Time Scoring Dashboard',
        style = {
            'textAlign': 'center',
            'colors': colors['text']
        }
    ),
    dcc.Graph(
        id = 'my-graph'
    ),
    html.Div(
        [
        # html.Label('Exploit vs Explore'),
        html.H2(
            children = "CONTROL CENTER",
            style = {
                'textAlign': 'center',
                'colors': colors['text']
            }
        ),
        dcc.Slider(
            id='hot-cold-slider',
            min=0,
            max=100,
            marks={i: 'Label {}'.format(i) if i == 1 else str(i) for i in range(0, 110, 10)},
            value=50
        ),
        # daq.GraduatedBar(
        #     id='hot-cold-bar',
        #     color={"ranges":{"blue":[0,4],"red":[7,10]}},
        #     showCurrentValue=True,
        #     value=10
        # )
        dash_table.DataTable(
            id='my-table',
            columns=[{"name": i, "id": i} for i in backup_df.columns],
            data=backup_df.to_dict('records')
        ),
        dcc.Interval(
            id='interval-component',
            interval=1 * 1000,  # in milliseconds
            n_intervals=0
        )
        ]
    )
])

# @app.callback(
#     Output("hot-cold-bar", "value"),
#     [Input("hot-cold-slider", "value")],
# )
# def update_output(cold_value):
#     return cold_value


# Multiple components can update everytime interval gets fired.
@app.callback([Output('my-graph', 'figure'),
               Output('my-table', 'columns'),
               Output('my-table', 'data')],
              [Input('interval-component', 'n_intervals')])
def update_graph_live(n):
    consumer.seek_to_end(topic_partition)
    records = consumer.poll(timeout_ms=0, max_records= 1)
    if len(records) == 0:
        print("NO RECORDS FOUND")

    df = json.loads(records[0])
    df['date'] = [datetime.datetime.fromtimestamp(ts / 1000) for ts in df.POST_TIMESTAMP]
    max_ts = max(df.POST_TIMESTAMP)
    df['tsnorm'] = [(ts - max_ts) / 1000 / 60 / 60 for ts in df.POST_TIMESTAMP]
    figure = go.Figure(
        data=[
            go.Bar(
                name='cold score',
                x=df['tsnorm'],
                # x = df['PROPERTIES_SHOPPABLE_POST_ID'],
                y=df['coldness_score'],
                width=0.1,
                # colors=colors['cold']
            ),
            go.Bar(
                name='hot score',
                x=df['tsnorm'],
                # x = df['PROPERTIES_SHOPPABLE_POST_ID'],
                y=df['hotness_score'],
                # colors=colors['hot']
                width=0.1,
            )
        ],
        layout=go.Layout(
            title='Post Scores in Past Three Days',
            barmode='stack',
            xaxis=dict(title='Hours Ago', range=[-6, 0]),
            yaxis=dict(title='Score')
        )
    )
    columns = [{"name": i, "id": i} for i in df.columns]
    data = df.to_dict('records')
    return figure, columns, data

if __name__ == '__main__':
    # main()
    app.run_server(host = '0.0.0.0', port = 8050, debug = True)

