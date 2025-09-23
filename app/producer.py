from kafka import KafkaProducer
import json

# Connexion au broker Kafka
producer = KafkaProducer(
    bootstrap_servers=['kafka:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Message JSON
message = {"msg": "Hello Kafka"}
producer.send('weather_stream', value=message)

producer.flush()
print("Message envoyé :", message)