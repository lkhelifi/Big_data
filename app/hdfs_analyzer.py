import json
from hdfs import InsecureClient
from datetime import datetime

class HDFSAnalyzer:
    def __init__(self):
        try:
            self.hdfs_client = InsecureClient('http://namenode:9870', user='root')
            print("Connexion HDFS établie")
        except Exception as e:
            print(f"Erreur connexion HDFS: {e}")
            self.hdfs_client = None
    
    def analyze_hdfs_data(self):
        """Analyse les données stockées dans HDFS"""
        if not self.hdfs_client:
            return
        
        try:
            base_path = "/hdfs-data"
            if not self.hdfs_client.status(base_path, strict=False):
                print("Aucune donnée trouvée dans /hdfs-data")
                return
            
            total_alerts = 0
            countries_stats = {}
            
            print("ANALYSE DES DONNÉES HDFS")
            print("=" * 50)
            
            countries = self.hdfs_client.list(base_path)
            for country in countries:
                country_path = f"{base_path}/{country}"
                cities = self.hdfs_client.list(country_path)
                
                countries_stats[country] = {
                    'cities': len(cities),
                    'total_alerts': 0,
                    'wind_alerts': {'level_1': 0, 'level_2': 0},
                    'heat_alerts': {'level_1': 0, 'level_2': 0}
                }
                
                print(f"\n{country.upper()} ({len(cities)} villes)")
                
                for city in cities:
                    city_path = f"{country_path}/{city}"
                    alerts_file = f"{city_path}/alerts.json"
                    
                    try:
                        if self.hdfs_client.status(alerts_file, strict=False):
                            with self.hdfs_client.read(alerts_file, encoding='utf-8') as reader:
                                content = reader.read()
                                if content.strip():
                                    alerts = json.loads(content)
                                    city_alert_count = len(alerts)
                                    total_alerts += city_alert_count
                                    countries_stats[country]['total_alerts'] += city_alert_count
                                    
                                    # Analyser les types d'alertes
                                    wind_l1 = wind_l2 = heat_l1 = heat_l2 = 0
                                    latest_temp = latest_wind = 0
                                    
                                    for alert in alerts:
                                        if alert.get('wind_alert_level') == 'level_1':
                                            wind_l1 += 1
                                        elif alert.get('wind_alert_level') == 'level_2':
                                            wind_l2 += 1
                                        
                                        if alert.get('heat_alert_level') == 'level_1':
                                            heat_l1 += 1
                                        elif alert.get('heat_alert_level') == 'level_2':
                                            heat_l2 += 1
                                        
                                        # Garder la dernière température et vitesse de vent
                                        latest_temp = alert.get('temperature', 0)
                                        latest_wind = alert.get('windspeed', 0)
                                    
                                    countries_stats[country]['wind_alerts']['level_1'] += wind_l1
                                    countries_stats[country]['wind_alerts']['level_2'] += wind_l2
                                    countries_stats[country]['heat_alerts']['level_1'] += heat_l1
                                    countries_stats[country]['heat_alerts']['level_2'] += heat_l2
                                    
                                    print(f"  {city}: {city_alert_count} alertes "
                                          f"(Dernier: {latest_temp}°C, {latest_wind}km/h)")
                    except Exception as e:
                        print(f"  Erreur lecture {city}: {e}")
            
            print("\n" + "=" * 50)
            print("RÉSUMÉ GLOBAL")
            print("=" * 50)
            print(f"Total des alertes: {total_alerts}")
            print(f"Pays surveillés: {len(countries_stats)}")
            
            for country, stats in countries_stats.items():
                print(f"\n{country}:")
                print(f"   Villes: {stats['cities']}")
                print(f"   Alertes totales: {stats['total_alerts']}")
                print(f"   Alertes vent - Niveau 1: {stats['wind_alerts']['level_1']}, "
                      f"Niveau 2: {stats['wind_alerts']['level_2']}")
                print(f"   Alertes chaleur - Niveau 1: {stats['heat_alerts']['level_1']}, "
                      f"Niveau 2: {stats['heat_alerts']['level_2']}")
        
        except Exception as e:
            print(f"Erreur analyse: {e}")
    
    def show_latest_alerts(self, limit=5):
        """Affiche les dernières alertes de chaque ville"""
        if not self.hdfs_client:
            return
        
        try:
            base_path = "/hdfs-data"
            if not self.hdfs_client.status(base_path, strict=False):
                return
            
            print(f"\nDERNIÈRES ALERTES (max {limit} par ville)")
            print("=" * 60)
            
            countries = self.hdfs_client.list(base_path)
            for country in countries:
                country_path = f"{base_path}/{country}"
                cities = self.hdfs_client.list(country_path)
                
                for city in cities:
                    alerts_file = f"{country_path}/{city}/alerts.json"
                    
                    try:
                        if self.hdfs_client.status(alerts_file, strict=False):
                            with self.hdfs_client.read(alerts_file, encoding='utf-8') as reader:
                                content = reader.read()
                                if content.strip():
                                    alerts = json.loads(content)
                                    latest_alerts = alerts[-limit:] if len(alerts) > limit else alerts
                                    
                                    print(f"\n{city}, {country}:")
                                    for alert in latest_alerts:
                                        timestamp = alert.get('timestamp', 'N/A')
                                        temp = alert.get('temperature', 0)
                                        wind = alert.get('windspeed', 0)
                                        wind_alert = alert.get('wind_alert_level', 'N/A')
                                        heat_alert = alert.get('heat_alert_level', 'N/A')
                                        
                                        print(f"    {timestamp}: {temp}°C, {wind}km/h "
                                              f"(Vent:{wind_alert}, Chaleur:{heat_alert})")
                    except Exception as e:
                        print(f"  Erreur lecture {city}: {e}")
        
        except Exception as e:
            print(f"Erreur affichage: {e}")
    
    def cleanup_old_data(self, days_to_keep=7):
        """Nettoie les anciennes données (optionnel)"""
        print(f"Fonction de nettoyage (garder {days_to_keep} jours) - À implémenter si nécessaire")

if __name__ == "__main__":
    analyzer = HDFSAnalyzer()
    
    print("ANALYSEUR HDFS WEATHER DATA")
    print("Choisissez une option:")
    print("1. Analyser toutes les données")
    print("2. Afficher les dernières alertes")
    print("3. Les deux")
    
    choice = input("Votre choix (1/2/3): ").strip()
    
    if choice in ['1', '3']:
        analyzer.analyze_hdfs_data()
    
    if choice in ['2', '3']:
        analyzer.show_latest_alerts()
    
    print("\nAnalyse terminée")