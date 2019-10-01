from builtins import range, min, len
import json
import datetime
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
#                             'size': 15,
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

def get_time_window_start(time_window_size: datetime.datetime) -> int:
    """
    Returns start of time window from now - time_window_size.

    :param time_window_size:
    :return: unix epoch time
    """
    time_window_start = datetime.datetime.now() - time_window_size
    time_window_start_epoch = int(time_window_start.timestamp() * 1000)
    return time_window_start_epoch


def consumer_get_latest_offset(consumer: KafkaConsumer) -> int:
    """
    Returns latest offset in topic of kafka consumer

    :param consumer: kafka consumer
    :return: kafka topic offset of last event within the time window of analysis
    """
    topic_partition = consumer.assignment().pop()
    end_offset = consumer.end_offsets([topic_partition])[topic_partition]
    return end_offset


def bootstrap_messages(consumer: KafkaConsumer, time_window_size: datetime.timedelta) -> dict:
    """
    Reads kafka topic as an event source to reconstitute a "snapshot" of
    scores for all posts by replaying them into a dictionary.

    Stores results in a dictionary where
        key: post_id
        value: dict of json

    :param consumer: kafka consumer that subscribes to relevant topic.
        REQUIRED to have exactly one partition
    :param time_window_size: post creation time window to include in analysis, relative to current time
    """
    time_window_start = datetime.datetime.now() - time_window_size
    time_window_start_epoch = int(time_window_start.timestamp() * 1000)
    consumer_seek_to_window_start(consumer, time_window_start)
    end_offset = consumer_get_latest_offset(consumer) - 1

    messages = {}
    counter = 0
    for m in consumer:
        if m is not None and m.value['POST_TIMESTAMP'] > time_window_start_epoch:
            messages[m.value['PROPERTIES_SHOPPABLE_POST_ID']] = m.value
            counter += 1
            if counter % 1000 == 0:
                print("message processing counter: %s, offset: %s" % (counter, m.offset))
        if m.offset >= end_offset:
            break
    return messages


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

scores = pd.DataFrame(data = None,
                      columns = ['POST_TIMESTAMP', 'PROPERTIES_SHOPPABLE_POST_ID',
                                 'PREVIEWS', 'FULL_VIEWS'])
score_lock = threading.Lock
time_window_size = datetime.timedelta(days=3)
time_window_start_epoch = get_time_window_start(time_window_size)

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
                             group_id='my-group' + str(random.randint(0, 1000000)),
                             value_deserializer=lambda x: json.loads(x.decode('utf-8')))

    # poll kafka topic until consumer partition is automatically generated

    # counter to trigger garbage collection
    counter = 0
    # start window to filter results by. updates periodically based on message counter

    messages = bootstrap_messages(consumer, time_window_size)


    for m in consumer:
        if m is not None and m.value['POST_TIMESTAMP'] > time_window_start_epoch:
            scores[m.value['PROPERTIES_SHOPPABLE_POST_ID']] = m.value['RT_SCORE']

            counter += 1
            if counter % 1000 == 0:
                counter = 1
                time_window_start = get_time_window_start(time_window_size)


        # get scores via event sourcing
        scores = bootstrap_messages(consumer, time_window_size)
        # last_push_timestamp = push_to_s3(scores)
        # consumer.close()

        # wait until the next push interval
        # next_push_timestamp = last_push_timestamp + push_interval
        # while datetime.datetime.now() < next_push_timestamp:
        #     sleep_duration = max((next_push_timestamp - datetime.datetime.now()).seconds, 1)
        #     print("sleeping until %s" % next_push_timestamp)
        #     print("sleeping for %s seconds" % sleep_duration)
        #     time.sleep(sleep_duration)




if __name__ == '__main__':
    main()
    # threading.Thread(target = main())
    # app.run_server(debug = True)


