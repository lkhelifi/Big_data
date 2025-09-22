import sys
from kafka import KafkaConsumer
import json

def main():
    if len(sys.argv) < 2:
        print("Usage: python consumer.py <topic>")
        sys.exit(1)

    topic = sys.argv[1]

    # Connexion au broker Kafka (ici via docker-compose)
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=['kafka:9092'],
        auto_offset_reset='earliest',  # lire depuis le début si jamais il y a déjà des messages
        enable_auto_commit=True,
        group_id="my-group",
        value_deserializer=lambda v: json.loads(v.decode('utf-8'))
    )

    print(f"📡 Consommateur démarré. Lecture en temps réel du topic '{topic}'...")

    # Boucle infinie de consommation
    for message in consumer:
        print(f"Message reçu: {message.value}")

if __name__ == "__main__":
    main()