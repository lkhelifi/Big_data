import json
import time
from datetime import datetime
from kafka import KafkaProducer

# Configuration du producteur Kafka
producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic = 'weather_transformed'

print(f"📡 Envoi de données simulées vers {topic}...")

try:
    counter = 0
    while counter < 10:  # Envoi de 10 messages pour test
        # Données simulées transformées
        data = {
            "temperature": 20 + (counter % 5),  # varie entre 20 et 24
            "windspeed": 15 + (counter % 8),    # varie entre 15 et 22
            "event_time": datetime.now().isoformat(),
            "wind_alert_level": "level_1" if (15 + (counter % 8)) < 20 else "level_2",
            "heat_alert_level": "level_0"  # température normale
        }
        
        print(f"Envoi: {data}")
        producer.send(topic, data)
        producer.flush()
        
        counter += 1
        time.sleep(2)  # Attendre 2 secondes entre chaque message
        
except KeyboardInterrupt:
    print("Arrêt du producteur")
finally:
    producer.close()
    print("Producteur fermé")