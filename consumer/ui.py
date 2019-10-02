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


def load_posts():
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

df = load_posts()

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets = external_stylesheets)

colors = {
    'background': '#111111',
    'text': '#7FDBFF'
}

app.layout = html.Div([
    html.H1(
        children = 'Real-Time Scoring Dashboard',
        style = {
            'textAlign': 'center',
            'colors': colors['text']
        }
    ),
    dash_table.DataTable(
        id='table',
        columns=[{"name": i, "id": i} for i in df.columns],
        data=df.to_dict('records')
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
        daq.GraduatedBar(
            id='hot-cold-bar',
            color={"ranges":{"blue":[0,4],"red":[7,10]}},
            showCurrentValue=True,
            value=10
        )
        ]
    )
])

@app.callback(
    Output("hot-cold-bar", "value"),
    [Input("hot-cold-slider", "value")],
)
def update_output(cold_value):
    return cold_value





#     dcc.Interval(
#         id='interval-component',
#         interval=1 * 1000,  # in milliseconds
#         n_intervals=0
#     ),
#     html.Div([
#         dcc.Graph(id='graph-with-slider'),
#         dcc.Slider(
#             id='year-slider',
#             min=df['year'].min(),
#             max=df['year'].max(),
#             value=df['year'].min(),
#             marks={str(year): str(year) for year in df['year'].unique()},
#             step=None
#         )
#     ]),
#     html.Div([
#         dcc.Input(id='my-id', value='initial value', type='text'),
#         html.Div(id='my-div')
#     ]),
#     html.H1(children='MillionArmBandit'),
#     html.Div(children='''
#         Faster content ranking optimization for better user feeds
#     '''),
#     dcc.Graph(
#         id = 'example-graph',
#         figure = {
#             'data': [
#                 {'x': [1, 2, 3], 'y': [4, 1, 2], 'type': 'bar', 'name': 'SF'},
#                 {'x': [1, 2, 3], 'y': [2, 4, 5], 'type': 'bar', 'name': 'Montreal'}
#             ],
#             'layout': {
#                 'title': 'Dash Data Visualization'
#             }
#         }
#     ),
#     html.H4(children = 'US Agriculture Exports (2011)'),
#     generate_table(df_for_agriculture_table),
#     html.H4(children = 'Life Expectancy vs GDP Graph'),
#     html.Div([
#         dcc.Graph(
#             id='life-exp-vs-gdp',
#             figure={
#                 'data': [
#                     go.Scatter(
#                         x=df_for_gdp_graph[df_for_gdp_graph['continent'] == i]['gdp per capita'],
#                         y=df_for_gdp_graph[df_for_gdp_graph['continent'] == i]['life expectancy'],
#                         text=df_for_gdp_graph[df_for_gdp_graph['continent'] == i]['country'],
#                         mode='markers',
#                         opacity=0.7,
#                         marker={
#                             'si`ze': 15,
#                             'line': {'width': 0.5, 'color': 'white'}
#                         },
#                         name=i
#                     ) for i in df_for_gdp_graph.continent.unique()
#                 ],
#                 'layout': go.Layout(
#                     xaxis={'type': 'log', 'title': 'GDP Per Capita'},
#                     yaxis={'title': 'Life Expectancy'},
#                     margin={'l': 40, 'b': 40, 't': 10, 'r': 10},
#                     legend={'x': 0, 'y': 1},
#                     hovermode='closest'
#                 )
#             }
#         )
#     ]),
#     html.Div([
#         dcc.Markdown(children = markdown_text)
#     ]),
#     html.Label('Dropdown'),
#     dcc.Dropdown(
#         options=[
#             {'label': 'New York City', 'value': 'NYC'},
#             {'label': u'Montréal', 'value': 'MTL'},
#             {'label': 'San Francisco', 'value': 'SF'}
#         ],
#         value='MTL'
#     ),
#
#     html.Label('Multi-Select Dropdown'),
#     dcc.Dropdown(
#         options=[
#             {'label': 'New York City', 'value': 'NYC'},
#             {'label': u'Montréal', 'value': 'MTL'},
#             {'label': 'San Francisco', 'value': 'SF'}
#         ],
#         value=['MTL', 'SF'],
#         multi=True
#     ),
#
#     html.Label('Radio Items'),
#     dcc.RadioItems(
#         options=[
#             {'label': 'New York City', 'value': 'NYC'},
#             {'label': u'Montréal', 'value': 'MTL'},
#             {'label': 'San Francisco', 'value': 'SF'}
#         ],
#         value='MTL'
#     ),
#
#     html.Label('Checkboxes'),
#     dcc.Checklist(
#         options=[
#             {'label': 'New York City', 'value': 'NYC'},
#             {'label': u'Montréal', 'value': 'MTL'},
#             {'label': 'San Francisco', 'value': 'SF'}
#         ],
#         value=['MTL', 'SF']
#     ),
#
#     html.Label('Text Input'),
#     dcc.Input(value='MTL', type='text'),
#
#     html.Label('Slider'),
#     dcc.Slider(
#         min=0,
#         max=9,
#         marks={i: 'Label {}'.format(i) if i == 1 else str(i) for i in range(1, 6)},
#         value=5,
#     )
# ])
#
# @app.callback(Output('live-update-text', 'children'),
#               [Input('interval-component', 'n_intervals')])
# def update_metrics(n):
#     pass
#
# @app.callback(
#     Output('hover-data', 'children'),
#     [Input('basic-interactions', 'hoverData')])
# def display_hover_data(hoverData):
#     return json.dumps(hoverData, indent=2)
#
#
# @app.callback(
#     Output('click-data', 'children'),
#     [Input('basic-interactions', 'clickData')])
# def display_click_data(clickData):
#     return json.dumps(clickData, indent=2)
#
#
# @app.callback(
#     Output('graph-with-slider', 'figure'),
#     [Input('year-slider', 'value')])
# def update_figure(selected_year):
#     filtered_df = df[df.year == selected_year]
#     traces = []
#     for i in filtered_df.continent.unique():
#         df_by_continent = filtered_df[filtered_df['continent'] == i]
#         traces.append(go.Scatter(
#             x = df_by_continent['gdpPercap'],
#             y = df_by_continent['lifeExp'],
#             text = df_by_continent['country'],
#             mode = 'markers',
#             opacity = 0.7,
#             marker = {
#                 'size': 15,
#                 'line': {'width': 0.5, 'color': 'white'}
#             },
#             name = i
#         ))
#     return {
#         'data': traces,
#         'layout': go.Layout(
#             xaxis={'type': 'log', 'title': 'GDP Per Capita'},
#             yaxis={'title': 'Life Expectancy', 'range': [20, 90]},
#             margin={'l': 40, 'b': 40, 't': 10, 'r': 10},
#             legend={'x': 0, 'y': 1},
#             hovermode='closest'
#         )
#     }


def generate_graph(posts: RecentPostsTable) -> dcc.Graph:
    pass





def generate_table(dataframe, max_rows=10):
    return html.Table(
        # Header
        [html.Tr([html.Th(col) for col in dataframe.columns])] +

        # Body
        [html.Tr([
            html.Td(dataframe.iloc[i][col]) for col in dataframe.columns
        ]) for i in range(min(len(dataframe), max_rows))]
    )


if __name__ == '__main__':
    # main()
    app.run_server(debug = True)
