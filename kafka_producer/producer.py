import requests
from kafka import KafkaProducer
import json
import time
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Load API key from environment variable
API_KEY = os.getenv('OPENWEATHER_API_KEY')
CITY = 'Kathmandu'
KAFKA_TOPIC = 'weather-data'
KAFKA_SERVER = 'localhost:9092'

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def fetch_weather_data():
    url = f'http://api.openweathermap.org/data/2.5/weather?q={CITY}&appid={API_KEY}'
    response = requests.get(url)
    return response.json()

while True:
    weather_data = fetch_weather_data()
    producer.send(KAFKA_TOPIC, weather_data)
    print(f"Sent data to Kafka: {weather_data}")
    time.sleep(60)  # Fetch data every 1 minute
