import json
import time
import requests
from datetime import datetime
from kafka import KafkaProducer

# Configuration du producteur Kafka
producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic = 'weather_transformed'

# Liste de villes avec leurs coordonnées pour avoir des données variées
cities = [
    {"name": "Paris", "country": "France", "lat": 48.8566, "lon": 2.3522},
    {"name": "London", "country": "UK", "lat": 51.5074, "lon": -0.1278},
    {"name": "New York", "country": "USA", "lat": 40.7128, "lon": -74.0060},
    {"name": "Tokyo", "country": "Japan", "lat": 35.6762, "lon": 139.6503},
    {"name": "Sydney", "country": "Australia", "lat": -33.8688, "lon": 151.2093},
    {"name": "Cairo", "country": "Egypt", "lat": 30.0444, "lon": 31.2357},
    {"name": "Moscow", "country": "Russia", "lat": 55.7558, "lon": 37.6176},
    {"name": "Mumbai", "country": "India", "lat": 19.0760, "lon": 72.8777}
]

def get_weather_data(city):
    """Récupère les données météo réelles depuis l'API Open-Meteo"""
    url = f"https://api.open-meteo.com/v1/forecast?latitude={city['lat']}&longitude={city['lon']}&current_weather=true"
    
    try:
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json().get("current_weather", {})
            if data:
                return {
                    "city": city["name"],
                    "country": city["country"],
                    "temperature": data.get("temperature", 0),
                    "windspeed": data.get("windspeed", 0),
                    "winddirection": data.get("winddirection", 0),
                    "weathercode": data.get("weathercode", 0),
                    "is_day": data.get("is_day", 1)
                }
    except Exception as e:
        print(f"❌ Erreur API pour {city['name']}: {e}")
    
    return None

def transform_weather_data(weather_data):
    """Applique les transformations (alertes) comme dans weather_transform.py"""
    if not weather_data:
        return None
    
    temperature = weather_data.get("temperature", 0)
    windspeed = weather_data.get("windspeed", 0)
    
    # Alertes de vent (même logique que weather_transform.py)
    if windspeed < 10:
        wind_alert_level = "level_0"
    elif windspeed <= 20:
        wind_alert_level = "level_1"
    else:
        wind_alert_level = "level_2"
    
    # Alertes de chaleur
    if temperature < 25:
        heat_alert_level = "level_0"
    elif temperature <= 35:
        heat_alert_level = "level_1"
    else:
        heat_alert_level = "level_2"
    
    # Données transformées finales
    transformed_data = {
        "city": weather_data["city"],
        "country": weather_data["country"],
        "temperature": temperature,
        "windspeed": windspeed,
        "winddirection": weather_data.get("winddirection", 0),
        "weathercode": weather_data.get("weathercode", 0),
        "is_day": weather_data.get("is_day", 1),
        "wind_alert_level": wind_alert_level,
        "heat_alert_level": heat_alert_level,
        "event_time": datetime.now().isoformat()
    }
    
    return transformed_data

print(f"🌍 Récupération de données météo réelles depuis l'API Open-Meteo...")
print(f"📡 Envoi vers le topic Kafka: {topic}")
print(f"🏙️  Villes surveillées: {len(cities)} villes dans le monde")

try:
    cycle = 0
    while True:  # Boucle infinie pour surveillance continue
        print(f"\n🔄 Cycle {cycle + 1} - Récupération des données météo...")
        
        for city in cities:
            # Récupérer les données météo réelles
            weather_data = get_weather_data(city)
            
            if weather_data:
                # Transformer les données (ajouter les alertes)
                transformed_data = transform_weather_data(weather_data)
                
                if transformed_data:
                    print(f"🌤️  {city['name']}: {transformed_data['temperature']}°C, "
                          f"{transformed_data['windspeed']}km/h "
                          f"(🌪️{transformed_data['wind_alert_level']}, "
                          f"🌡️{transformed_data['heat_alert_level']})")
                    
                    # Envoyer vers Kafka
                    producer.send(topic, transformed_data)
                    producer.flush()
                    
                    # Petite pause entre chaque ville
                    time.sleep(1)
        
        cycle += 1
        print(f"✅ Cycle {cycle} terminé. Pause de 30 secondes avant le prochain cycle...")
        time.sleep(30)  # Attendre 30 secondes avant le prochain cycle complet
        
except KeyboardInterrupt:
    print("\n🛑 Arrêt du producteur (Ctrl+C détecté)")
finally:
    producer.close()
    print("✅ Producteur fermé proprement")