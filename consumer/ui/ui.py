from builtins import range, min, len

import math
import threading
import dash
import dash_core_components as dcc
import dash_html_components as html
import pandas as pd
import plotly.graph_objs as go
from dash.dependencies import Input, Output
from kafka import KafkaConsumer
import smart_open
import json
import time
import datetime
from pytz import timezone
import random
import pdb

# markdown_text = '''
# ### Dash and Markdown
#
# Dash apps can be written in Markdown.
# Dash uses the [CommonMark] (http://commonmark.org/)
# specification of Markdown
# '''
#
# df_for_gdp_graph = pd.read_csv(
#     'https://gist.githubusercontent.com/chriddyp/' +
#     '5d1ea79569ed194d432e56108a04d188/raw/' +
#     'a9f9e8076b837d541398e999dcbac2b2826a81f8/'+
#     'gdp-life-exp-2007.csv')
#
# df_for_agriculture_table = pd.read_csv(
#     'https://gist.githubusercontent.com/chriddyp/'
#     'c78bf172206ce24f77d6363a2d754b59/raw/'
#     'c353e8ef842413cae56ae3920b8fd78468aa4cb2/'
#     'usa-agricultural-exports-2011.csv')
#
# df = pd.read_csv(
#     'https://raw.githubusercontent.com/plotly/'
#     'datasets/master/gapminderDataFiveYear.csv')
#
#
#
# def generate_table(dataframe, max_rows=10):
#     return html.Table(
#         # Header
#         [html.Tr([html.Th(col) for col in dataframe.columns])] +
#
#         # Body
#         [html.Tr([
#             html.Td(dataframe.iloc[i][col]) for col in dataframe.columns
#         ]) for i in range(min(len(dataframe), max_rows))]
#     )
#
# external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
#
# app = dash.Dash(__name__, external_stylesheets = external_stylesheets)
#
# app.layout = html.Div(children = [
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
#
#
# @app.callback(
#     Output(component_id = 'my-div', component_property = 'children'),
#     [Input(component_id = 'my-id', component_property = 'value')]
# )
# def update_output_div(input_value):
#     return 'You\'ve entered "{}"'.format(input_value)


class ScoringFunctionCreator:
    def __init__(self, max_coldness_score=50, min_previews_threshold=30, cold_threshold_steepness=0.5,
                 max_hotness_score=50, ctr_hotness_threshold=0.12, hot_threshold_steepness=20):
        self.max_coldness_score = max_coldness_score
        self.min_previews_threshold = min_previews_threshold
        self.cold_threshold_steepness = cold_threshold_steepness
        self.max_hotness_score = max_hotness_score
        self.ctr_hotness_threshold = ctr_hotness_threshold
        self.hot_threshold_steepness = hot_threshold_steepness

    def score(self, previews, full_views):
        return self.hotness_score(previews, full_views) + self.coldness_score(previews, full_views)

    def hotness_score(self, previews, full_views):
        if previews + full_views == 0:
            return 0
        # max fn guards against edge case of out of ordering of preview and view event delivery
        click_thru_rate = full_views / max(previews, full_views)
        hotness_weight = 1.0 / (1.0 + math.exp(-self.hot_threshold_steepness * (click_thru_rate - self.ctr_hotness_threshold)))
        return hotness_weight * self.max_hotness_score

    def coldness_score(self, previews):
        coldness_weight = 1 - 1 / (1 + math.exp( -self.cold_threshold_steepness * (previews - self.min_previews_threshold)))
        return coldness_weight * self.max_coldness_score


class RecentPostsTable:
    def __init__(self, consumer: KafkaConsumer,
                 scoring_function=ScoringFunctionCreator(),
                 time_window_size=datetime.timedelta(days=3)):
        self.consumer = consumer
        self.scoring_function = scoring_function
        self.time_window_size = time_window_size
        self.time_window_start = None
        self.time_window_start_epoch = None
        self.topic_partition = None
        self.__seek_to_window_start() #initializes time_window_start, time_window_start_epoch, and topic_partition
        self.posts = {}
        self.__bulk_consume_events()

    def get_snapshot(self) -> pd.DataFrame:
        """

        :return:
        """
        self.__garbage_collect_old()
        self.__bulk_consume_events()
        self.__apply_score()
        return pd.DataFrame.from_dict(self.scores, orient='index')

    def update_scoring_function(self, scoring_function):
        self.scoring_function = scoring_function
        self.__apply_score()

    def __apply_score(self):
        for key, json_dict in self.posts.items():
            json_dict['score'] = self.scoring_function.score(json_dict['PREVIEWS'], json_dict['FULL_VIEWS'])
            json_dict['coldness_score'] = self.scoring_function.coldness_score(json_dict['PREVIEWS'])
            json_dict['hotness_score'] = self.scoring_function.hotness_score()

    def __bulk_consume_events(self):
        """
        Reads kafka topic as an event source to reconstitute a "snapshot" of
        scores for all posts by replaying them into a dictionary.

        """
        end_offset = self.consumer.end_offsets(self.topic_partition) - 1
        for m in self.consumer:
            if m is not None and m.value['POST_TIMESTAMP'] > self.time_window_start_epoch:
                self.posts[m.value['PROPERTIES_SHOPPABLE_POST_ID']] = m.value
            if m.offset >= end_offset:
                break

    def __garbage_collect_old(self):
        """
        Removes all tracked posts
        :return:
        """
        #TODO: Refactor with a secondary index, ordered by creation timestamp.
        for post_id in list(self.posts.keys()):
            if self.posts[post_id]['POST_TIMESTAMP'] < self.time_window_start_epoch:
                self.posts.pop(post_id)

    def __seek_to_window_start(self):
        """
        This function mutates the consumer to "seek" the kafka topic offset to that of the earliest event that
        is inside the time_window.
        """
        self.__update_time_window_start()
        if len(self.consumer.assignment()) == 0:
            # poll consumer to generate a topic partition assignment
            message = self.consumer.poll(1, 1)
            while len(message) == 0:
                message = self.consumer.poll(1, 1)
        self.topic_partition = self.consumer.assignment().pop()
        time_window_start_epoch = int(self.time_window_start.timestamp()*1000)

        # get first offset that is in the time window
        start_offset = self.consumer.offsets_for_times({self.topic_partition: time_window_start_epoch})[self.topic_partition].offset
        # set the consumer to consume from this offset
        self.consumer.seek(self.topic_partition, start_offset)

    def __update_time_window_start(self):
        """
        Returns start of time window from now - self.time_window_size.

        """
        self.time_window_start = datetime.datetime.now() - self.time_window_size
        self.time_window_start_epoch = int(self.time_window_start.timestamp() * 1000)

def main():
    # config variables
    # TODO: refactor to take these in commandline
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
    posts = RecentPostsTable(consumer, scoring_function)
    df = posts.get_snapshot()




        # last_push_timestamp = push_to_s3(scores)
        # consumer.close()

        # wait until the next push interval
        # next_push_timestamp = last_push_timestamp + push_interval
        # while datetime.datetime.now() < next_push_timestamp:
        #     sleep_duration = max((next_push_timestamp - datetime.datetime.now()).seconds, 1)
        #     print("sleeping until %s" % next_push_timestamp)
        #     print("sleeping for %s seconds" % sleep_duration)
        #     time.sleep(sleep_duration)


df = pd.read_csv(
    'https://gist.githubusercontent.com/chriddyp/'
    'c78bf172206ce24f77d6363a2d754b59/raw/'
    'c353e8ef842413cae56ae3920b8fd78468aa4cb2/'
    'usa-agricultural-exports-2011.csv')



def generate_table(dataframe, max_rows=10):
    return html.Table(
        # Header
        [html.Tr([html.Th(col) for col in dataframe.columns])] +

        # Body
        [html.Tr([
            html.Td(dataframe.iloc[i][col]) for col in dataframe.columns
        ]) for i in range(min(len(dataframe), max_rows))]
    )

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets = external_stylesheets)
app.layout = html.Div(children = [
    html.H1(children='MillionArmBandit'),
    html.Div(children='''
        Faster ranking optimization for content feeds
    '''),
    dcc.Graph(
        id = 'example-graph',
        figure = {
            'data': [
                {'x': [1, 2, 3], 'y': [4, 1, 2], 'type': 'bar', 'name': 'SF'},
                {'x': [1, 2, 3], 'y': [2, 4, 5], 'type': 'bar', 'name': 'Montreal'}
            ],
            'layout': {
                'title': 'Dash Data Visualization'
            }
        }
    ),
    html.H4(children = 'US Agriculture Exports (2011)'),
    generate_table(df)
])

if __name__ == '__main__':
    main()
    app.run_server(debug = True)
