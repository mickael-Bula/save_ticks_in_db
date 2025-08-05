import os
import datetime
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from typing import Optional

# Charge les variables d'environnement
dotenv_file = ".env.local"
load_dotenv(dotenv_path=dotenv_file)

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")


def get_db_engine() -> Engine:
    """Crée et retourne l'objet moteur de la base de données."""
    if not all([DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME]):
        raise ValueError(
            f"Erreur : Les variables de connexion à la base de données "
            + "ne sont pas définies dans le fichier {dotenv_file}.")

    return create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")


def create_table_if_not_exists(engine: Engine, table_name: str) -> None:
    """Crée une table pour le ticker si elle n'existe pas déjà."""
    with engine.connect() as conn:
        create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS "{table_name}" (
                date TIMESTAMP PRIMARY KEY,
                open FLOAT,
                high FLOAT,
                low FLOAT,
                close FLOAT,
                adj_close FLOAT,
                volume BIGINT
            );
        """
        conn.execute(text(create_table_sql))
        conn.commit()


def get_last_date(engine: Engine, table_name: str) -> Optional[datetime.datetime]:
    """Récupère la dernière date de l'historique pour un ticker."""
    with engine.connect() as conn:
        query = f'SELECT "date" FROM "{table_name}" ORDER BY "date" DESC LIMIT 1'
        result = conn.execute(text(query)).fetchone()
        if result:
            return result[0]
        return None
    