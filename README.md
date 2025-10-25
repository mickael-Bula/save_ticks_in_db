## Installation d'un paquet depuis Pycharm

L'installation d'un paquet peut se faire avec la commande `pip install`,
après s'être assuré que l'environnement virtuel est bien activé.

Mais il est aussi possible d'installer une librairie Python requise par le projet,
sans passer par le terminal et l'activation de l'environnement virtuel. Pour cela :

```
CTRL + ALT + S > Project (nom du projet) > Python Interpreter > + > saisir le paquet
```

## Création de la BDD postgres

Le script échouant à créer la base, celle-ci doit être générée en SQL.
Mon moteur postgresql se trouvant dans Laragon, je dois créer la base avec HeidiSQL.

Pour cela, je commence par créer une session **yfinance** en conservant le mdp et le user par défaut.
Je précise que je veux ouvrir cette nouvelle session sur la base **yfinance_db**.

Une fois la session ouverte sur cette base, j'exécute la requête suivante :

```SQL
CREATE DATABASE yfinance_db WITH ENCODING 'UTF8' LC_COLLATE 'French_France.1252' LC_CTYPE 'French_France.1252' TEMPLATE template0;
```

Cela crée la base en utilisant l'encodage UTF-8 attendu par SqlAlchemy,
tout en utilisant la collation déclarée dans HeidiSQL (French).

## Insertion des données

L'exécution du script **insert_candlesticks.py** génère la table **cac** avec ses données.

## Vérification de la donnée du jour

Pour éviter d'insérer des données incomplètes en base,
la donnée du jour n'est récupérée qu'après 18h un jour de semaine.

## Déclarer les variables d'environnement dans un fichier .env

Il faut installer la librairie **dotenv** :

```bash
$ pip install python-dotenv
```

Créer un fichier `.env` pour y déclarer les variables,
puis appeler celles-ci avec la méthode `os.getenv` :

```python
from dotenv import load_dotenv

load_dotenv()

# Récupère les variables d'environnement
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
```

