import sys
import json
import requests
from kafka import KafkaProducer
from time import sleep
from datetime import datetime

# Vérification des arguments
if len(sys.argv) != 3:
    print("Usage: python current_weather_v2.py <city> <country>")
    sys.exit(1)

city = sys.argv[1]
country = sys.argv[2]

# Appel à l'API de géocodage Open-Meteo
geo_url = f"https://geocoding-api.open-meteo.com/v1/search?name={city}&country={country}&count=1"
geo_resp = requests.get(geo_url)
if geo_resp.status_code != 200 or not geo_resp.json().get("results"):
    print(f"Impossible de géocoder {city}, {country}")
    sys.exit(1)

location = geo_resp.json()["results"][0]
latitude = location["latitude"]
longitude = location["longitude"]

# URL API météo
weather_url = f"https://api.open-meteo.com/v1/forecast?latitude={latitude}&longitude={longitude}&current_weather=true"

# Configuration du producteur Kafka
producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic = 'weather_stream'

try:
    while True:
        response = requests.get(weather_url)
        if response.status_code == 200:
            data = response.json().get("current_weather", {})
            if data:
                message = {
                    "city": city,
                    "country": country,
                    "temperature": data.get("temperature"),
                    "windspeed": data.get("windspeed"),
                    "event_time": datetime.utcnow().isoformat()
                }
                print(f"Envoi des données : {message}")
                producer.send(topic, message)
        else:
            print(f"Erreur API météo : {response.status_code}")
        sleep(60)  # toutes les 60 secondes
except KeyboardInterrupt:
    print("Arrêt du producteur")
finally:
    producer.close()