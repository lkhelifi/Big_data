import sys
import json
import requests
from kafka import KafkaProducer

# Vérification des arguments
if len(sys.argv) != 3:
    print("Usage: python current_weather.py <latitude> <longitude>")
    sys.exit(1)

latitude = sys.argv[1]
longitude = sys.argv[2]

# URL Open-Meteo
url = f"https://api.open-meteo.com/v1/forecast?latitude={latitude}&longitude={longitude}&current_weather=true"

# Configuration du producteur Kafka
producer = KafkaProducer(
    bootstrap_servers='kafka:9092',  # Même BROKER que le consommateur
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic = 'weather_stream'

try:
    while True:
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            current = data.get("current_weather", {})
            print(f"Envoi des données : {current}")
            producer.send(topic, current)
        else:
            print(f"Erreur API : {response.status_code}")

except KeyboardInterrupt:
    print("Arrêt du producteur")
finally:
    producer.close()