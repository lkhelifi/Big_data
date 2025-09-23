import json
import os
from kafka import KafkaConsumer
from hdfs import InsecureClient
from datetime import datetime

class HDFSWeatherConsumer:
    def __init__(self):
        # Configuration du consumer Kafka
        self.consumer = KafkaConsumer(
            'weather_transformed',
            bootstrap_servers=['kafka:9092'],
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id="hdfs-weather-group",
            value_deserializer=lambda v: json.loads(v.decode('utf-8'))
        )
        
        # Configuration du client HDFS
        try:
            self.hdfs_client = InsecureClient('http://namenode:9870', user='root')
            print("Connexion HDFS établie")
        except Exception as e:
            print(f"Erreur connexion HDFS: {e}")
            self.hdfs_client = None
    
    def create_hdfs_directory(self, path):
        """Crée un répertoire HDFS s'il n'existe pas"""
        try:
            if not self.hdfs_client.status(path, strict=False):
                self.hdfs_client.makedirs(path)
                print(f"Répertoire créé: {path}")
            return True
        except Exception as e:
            print(f"Erreur création répertoire {path}: {e}")
            return False
    
    def save_alert_to_hdfs(self, data):
        """Sauvegarde une alerte dans HDFS avec la structure organisée"""
        if not self.hdfs_client:
            print("Client HDFS non disponible")
            return False
        
        try:
            # Extraire les informations nécessaires
            country = data.get('country', 'unknown').replace(' ', '_')
            city = data.get('city', 'unknown').replace(' ', '_')
            
            # Créer la structure de répertoires
            base_path = f"/hdfs-data/{country}/{city}"
            
            if not self.create_hdfs_directory(base_path):
                return False
            
            # Préparer les données d'alerte
            alert_data = {
                "timestamp": datetime.now().isoformat(),
                "city": data.get('city'),
                "country": data.get('country'),
                "temperature": data.get('temperature'),
                "windspeed": data.get('windspeed'),
                "wind_alert_level": data.get('wind_alert_level'),
                "heat_alert_level": data.get('heat_alert_level'),
                "event_time": data.get('event_time'),
                "weathercode": data.get('weathercode'),
                "is_day": data.get('is_day'),
                "winddirection": data.get('winddirection')
            }
            
            # Chemin complet du fichier
            file_path = f"{base_path}/alerts.json"
            
            # Lire le fichier existant ou créer un nouveau
            existing_alerts = []
            try:
                if self.hdfs_client.status(file_path, strict=False):
                    with self.hdfs_client.read(file_path, encoding='utf-8') as reader:
                        content = reader.read()
                        if content.strip():
                            existing_alerts = json.loads(content)
            except Exception as e:
                print(f"Fichier {file_path} n'existe pas encore, création d'un nouveau")
            
            # Ajouter la nouvelle alerte
            existing_alerts.append(alert_data)
            
            # Garder seulement les 100 dernières alertes par ville pour éviter des fichiers trop gros
            if len(existing_alerts) > 100:
                existing_alerts = existing_alerts[-100:]
            
            # Écrire dans HDFS
            json_content = json.dumps(existing_alerts, indent=2, ensure_ascii=False)
            
            with self.hdfs_client.write(file_path, encoding='utf-8', overwrite=True) as writer:
                writer.write(json_content)
            
            print(f"Alerte sauvegardée: {file_path}")
            return True
            
        except Exception as e:
            print(f"Erreur sauvegarde HDFS: {e}")
            return False
    
    def should_save_alert(self, data):
        """Détermine si une alerte doit être sauvegardée"""
        wind_alert = data.get('wind_alert_level', 'level_0')
        heat_alert = data.get('heat_alert_level', 'level_0')
        
        # Sauvegarder seulement les alertes level_1 et level_2
        return wind_alert in ['level_1', 'level_2'] or heat_alert in ['level_1', 'level_2']
    
    def list_hdfs_structure(self):
        """Affiche la structure des données dans HDFS"""
        try:
            base_path = "/hdfs-data"
            if self.hdfs_client.status(base_path, strict=False):
                print("\nStructure HDFS actuelle:")
                countries = self.hdfs_client.list(base_path)
                for country in countries:
                    country_path = f"{base_path}/{country}"
                    cities = self.hdfs_client.list(country_path)
                    print(f"{country}/")
                    for city in cities:
                        city_path = f"{country_path}/{city}"
                        files = self.hdfs_client.list(city_path)
                        for file in files:
                            file_path = f"{city_path}/{file}"
                            status = self.hdfs_client.status(file_path)
                            size = status['length']
                            print(f"  {city}/{file} ({size} bytes)")
        except Exception as e:
            print(f"Erreur listage HDFS: {e}")
    
    def run(self):
        """Lance le consumer principal"""
        print("Démarrage du consumer HDFS Weather...")
        print("Lecture du topic: weather_transformed")
        print("Sauvegarde dans HDFS: /hdfs-data/{country}/{city}/alerts.json")
        print("Filtre: Seulement les alertes level_1 et level_2")
        print("-" * 60)
        
        message_count = 0
        alert_count = 0
        
        try:
            for message in self.consumer:
                data = message.value
                message_count += 1
                
                city = data.get('city', 'Unknown')
                country = data.get('country', 'Unknown')
                temp = data.get('temperature', 0)
                wind = data.get('windspeed', 0)
                wind_alert = data.get('wind_alert_level', 'level_0')
                heat_alert = data.get('heat_alert_level', 'level_0')
                
                print(f"[{message_count}] {city}, {country}: {temp}°C, {wind}km/h "
                      f"(Vent:{wind_alert}, Chaleur:{heat_alert})")
                
                # Vérifier s'il faut sauvegarder cette alerte
                if self.should_save_alert(data):
                    if self.save_alert_to_hdfs(data):
                        alert_count += 1
                        print(f"-> Alerte #{alert_count} sauvegardée dans HDFS")
                else:
                    print("   Pas d'alerte à sauvegarder (level_0)")
                
                # Afficher la structure HDFS toutes les 10 alertes
                if alert_count > 0 and alert_count % 10 == 0:
                    self.list_hdfs_structure()
                
                print("-" * 40)
                
        except KeyboardInterrupt:
            print(f"\nArrêt du consumer (Ctrl+C détecté)")
            print(f"Statistiques:")
            print(f"   Messages traités: {message_count}")
            print(f"   Alertes sauvegardées: {alert_count}")
            
        finally:
            self.consumer.close()
            print("Consumer fermé proprement")

if __name__ == "__main__":
    # Installer hdfs si nécessaire
    try:
        import hdfs
    except ImportError:
        print("Installation de la bibliothèque hdfs...")
        os.system("pip install hdfs")
        import hdfs
    
    consumer = HDFSWeatherConsumer()
    consumer.run()