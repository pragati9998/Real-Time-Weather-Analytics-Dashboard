from dash import Dash, dcc, html
from dash.dependencies import Input, Output
import plotly.graph_objs as go
from kafka import KafkaConsumer
import threading
import json
import time

app = Dash(__name__)

# Shared data structure to hold the consumed messages
weather_data_list = []

# Function to consume messages from Kafka
def consume_weather_data():
    consumer = KafkaConsumer(
        'weather-data',
        bootstrap_servers='localhost:9092',
        value_deserializer=lambda v: json.loads(v.decode('utf-8'))
    )
    for message in consumer:
        weather_data = message.value
        weather_data_list.append(weather_data)

# Start Kafka consumer in a separate thread
consumer_thread = threading.Thread(target=consume_weather_data)
consumer_thread.start()

app.layout = html.Div([
    dcc.Graph(id='live-weather-graph', animate=True),
    dcc.Interval(
        id='graph-update',
        interval=10000,  # Update every 10 seconds
        n_intervals=0
    ),
])

@app.callback(Output('live-weather-graph', 'figure'),
              [Input('graph-update', 'n_intervals')])
def update_graph_scatter(n):
    # Copy data to avoid race conditions
    data = []
    temp_data = []
    humidity_data = []
    
    for weather_data in weather_data_list[-10:]:  # Display last 10 readings
        temp_c = weather_data['main']['temp'] - 273.15
        city = weather_data['name']
        temp_data.append((city, temp_c))
        humidity_data.append((city, weather_data['main']['humidity']))

    # Creating traces
    temp_trace = go.Scatter(
        x=[city for city, _ in temp_data],
        y=[temp for _, temp in temp_data],
        mode='lines+markers',
        name='Temperature (Celsius)'
    )
    humidity_trace = go.Scatter(
        x=[city for city, _ in humidity_data],
        y=[humidity for _, humidity in humidity_data],
        mode='lines+markers',
        name='Humidity (%)'
    )

    return {
        'data': [temp_trace, humidity_trace],
        'layout': go.Layout(title='Real-Time Weather Data',
                            xaxis=dict(title='City'),
                            yaxis=dict(title='Values'))
    }

if __name__ == '__main__':
    app.run_server(debug=True)
