import json
from kafka import KafkaConsumer

# Connexion au broker Kafka
consumer = KafkaConsumer(
    'weather_stream',
    bootstrap_servers=['kafka:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id="weather-group",
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

print("📡 Consumer démarré pour weather_stream...")

# Boucle de consommation
for message in consumer:
    data = message.value
    print(f"🌤️  Données météo reçues: {data}")
    
    # Transformation simple (comme dans weather_transform.py)
    temperature = data.get('temperature', 0)
    windspeed = data.get('windspeed', 0)
    
    # Alertes de vent
    if windspeed < 10:
        wind_alert = "level_0"
    elif windspeed <= 20:
        wind_alert = "level_1"
    else:
        wind_alert = "level_2"
    
    # Alertes de chaleur
    if temperature < 25:
        heat_alert = "level_0"
    elif temperature <= 35:
        heat_alert = "level_1"
    else:
        heat_alert = "level_2"
    
    print(f"🌡️  Température: {temperature}°C (alerte: {heat_alert})")
    print(f"💨 Vitesse vent: {windspeed} km/h (alerte: {wind_alert})")
    print("-" * 50)