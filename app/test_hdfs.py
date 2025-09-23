import subprocess
import sys

def install_hdfs():
    """Installe le module hdfs s'il n'est pas présent"""
    try:
        import hdfs
        return True
    except ImportError:
        print("Installation du module hdfs...")
        try:
            subprocess.check_call([sys.executable, "-m", "pip", "install", "hdfs"])
            import hdfs
            print("Module hdfs installé avec succès")
            return True
        except Exception as e:
            print(f"Erreur installation hdfs: {e}")
            return False

# Installer hdfs si nécessaire
if not install_hdfs():
    print("Impossible d'installer le module hdfs")
    sys.exit(1)

from hdfs import InsecureClient
import json

def test_hdfs_connection():
    """Test la connexion HDFS et crée la structure de base"""
    try:
        # Connexion HDFS
        client = InsecureClient('http://namenode:9870', user='root')
        print("Connexion HDFS réussie")
        
        # Test de création de répertoire
        test_path = "/hdfs-data"
        if not client.status(test_path, strict=False):
            client.makedirs(test_path)
            print(f"Répertoire {test_path} créé")
        else:
            print(f"Répertoire {test_path} existe déjà")
        
        # Test d'écriture
        test_file = "/hdfs-data/test.json"
        test_data = {
            "message": "Test HDFS",
            "timestamp": "2025-09-23",
            "status": "success"
        }
        
        with client.write(test_file, encoding='utf-8', overwrite=True) as writer:
            writer.write(json.dumps(test_data, indent=2))
        print(f"Fichier test écrit: {test_file}")
        
        # Test de lecture
        with client.read(test_file, encoding='utf-8') as reader:
            content = reader.read()
            data = json.loads(content)
            print(f"Fichier test lu: {data['message']}")
        
        # Nettoyage
        client.delete(test_file)
        print(f"Fichier test supprimé")
        
        # Afficher les informations HDFS
        status = client.status('/')
        print(f"HDFS Root Status: {status}")
        
        return True
        
    except Exception as e:
        print(f"Erreur HDFS: {e}")
        return False

if __name__ == "__main__":
    print("TEST DE CONNEXION HDFS")
    print("-" * 30)
    
    if test_hdfs_connection():
        print("\nHDFS est prêt pour le stockage des alertes météo !")
    else:
        print("\nProblème avec HDFS - Vérifiez la configuration")