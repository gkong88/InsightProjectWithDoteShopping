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


backup_df = pd.read_pickle('sample_data')
backup_df['date'] = [datetime.datetime.fromtimestamp(ts / 1000) for ts in backup_df.POST_TIMESTAMP]
max_ts = max(backup_df.POST_TIMESTAMP)
backup_df['tsnorm'] = [(ts - max_ts) / 1000 / 60 / 60 for ts in backup_df.POST_TIMESTAMP]


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
    df = backup_df
    #df = posts.get_snapshot()
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
    app.run_server(host = '0.0.0.0', debug = True)

