# api_url = "http://www.weatherapi.com/your-api-endpoint"
# api_key = "14cf4b76103f439a9df70759231010"
# api_url = f'https://api.weatherapi.com/v1/current.json?key={api_key}&q={city}'
#     response = requests.get(api_url,verify=False)

import requests
from confluent_kafka import Producer
import json

# Define your Weather API endpoint and API key
api_key = "14cf4b76103f439a9df70759231010"

# Define your Kafka producer configuration
kafka_config = {
    'bootstrap.servers': 'localhost:9092',
}

# Define your Kafka topic and the number of partitions
kafka_topic = "weatherdata"
num_partitions = 5

# Create a Kafka producer instance
producer = Producer(kafka_config)

# Define a function to fetch specific weather data for a city
def fetch_weather_data(city):
    api_url = f'https://api.weatherapi.com/v1/current.json?key={api_key}&q={city}'
    response = requests.get(api_url, verify=False)
    weather_data = response.json()
    data_to_produce = {
        "location": {
            "name": weather_data["location"]["name"],
            "country": weather_data["location"]["country"],
            "lat": weather_data["location"]["lat"],
            "lon": weather_data["location"]["lon"],
            "localtime_epoch": weather_data["location"]["localtime_epoch"],
            "localtime": weather_data["location"]["localtime"]
        },
        "current": {
            "temp_c": weather_data["current"]["temp_c"],
            "temp_f": weather_data["current"]["temp_f"],
            "condition": {
                "text": weather_data["current"]["condition"]["text"]
            },
            "wind_mph": weather_data["current"]["wind_mph"],
            "wind_kph": weather_data["current"]["wind_kph"],
            "precip_mm": weather_data["current"]["precip_mm"],
            "precip_in": weather_data["current"]["precip_in"],
            "gust_mph": weather_data["current"]["gust_mph"],
            "gust_kph": weather_data["current"]["gust_kph"]
        }
    }
    return data_to_produce

# Define a Kafka producer function
def produce_to_kafka(city, data):
    key = city
    value = json.dumps(data)
    partition = hash(city) % num_partitions  # Distribute data evenly
    producer.produce(topic=kafka_topic, key=key, value=value)

# List of cities to fetch data for
cities = ["London", "New York", "Paris", "Tokyo", "Sydney"]

# Fetch specific data and produce to Kafka
for city in cities:
    data = fetch_weather_data(city)
    produce_to_kafka(city, data)

# Flush the producer buffer to ensure all messages are sent
producer.flush()


