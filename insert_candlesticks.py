import yfinance as yf
import pandas as pd
import datetime
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
import os
from dotenv import load_dotenv
from typing import Optional

# Charge les variables d'environnement du fichier .env ou .env.local.
dotenv_file = ".env.local"
load_dotenv(dotenv_path=dotenv_file)

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

if not all([DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME]):
    print(
        f"Erreur : Les variables de connexion à la base de données ne sont pas définies dans le fichier {dotenv_file}.")
    exit()

engine: Engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")


def create_table_if_not_exists(table_name: str) -> None:
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


def get_last_date(table_name: str) -> Optional[datetime.datetime]:
    """Récupère la dernière date de l'historique pour un ticker."""
    with engine.connect() as conn:
        query = f'SELECT "date" FROM "{table_name}" ORDER BY "date" DESC LIMIT 1'
        result = conn.execute(text(query)).fetchone()
        if result:
            return result[0]
        return None


def load_data_to_db(ticker: str, table_name: str, start_date: Optional[datetime.datetime] = None) -> None:
    """
    Récupère les données depuis yfinance et les charge dans la base de données.
    Si start_date est fourni, récupère les données à partir de cette date.
    """
    print(f"Chargement des données pour {ticker}...")

    if start_date:
        print(f"Récupération des données depuis le {start_date.strftime('%Y-%m-%d')}")
        data = yf.download(ticker, start=start_date, auto_adjust=False)
    else:
        print("Première récupération : chargement de l'historique complet.")
        data = yf.download(ticker, period="max", auto_adjust=False)

    if data.empty:
        print("Aucune nouvelle donnée à charger.")
        return

    if isinstance(data.columns, pd.MultiIndex):
        data.columns = data.columns.get_level_values(0)

    data.columns = data.columns.str.lower()
    data.columns = data.columns.str.replace(' ', '_')

    price_cols = ['open', 'high', 'low', 'close', 'adj_close']
    for col in price_cols:
        if col in data.columns:
            data[col] = data[col].fillna(0).astype('float64')

    if 'volume' in data.columns:
        data['volume'] = data['volume']

    data.index.name = "date"
    data = data.reset_index()

    current_time = datetime.datetime.now()
    is_weekday = 0 <= current_time.weekday() <= 4
    is_before_market_close = current_time.hour < 18

    if is_weekday and is_before_market_close:
        if not data.empty and data['date'].iloc[-1].date() == current_time.date():
            print("Le marché est ouvert en semaine. La dernière ligne de données (non finale) ne sera pas insérée.")
            data = data.iloc[:-1]
            if data.empty:
                print("Le DataFrame est vide après exclusion de la dernière ligne. Fin du script.")
                return

    with engine.connect() as conn:
        for index, row in data.iterrows():
            date_to_check = row['date']

            volume_value = int(row['volume']) if pd.notna(row['volume']) and row['volume'] > 0 else None

            check_query = text(f'SELECT 1 FROM "{table_name}" WHERE "date" = :date_val')
            result = conn.execute(check_query, {'date_val': date_to_check}).fetchone()

            if result:
                update_query = text(f"""
                    UPDATE "{table_name}"
                    SET open = :open, high = :high, low = :low, close = :close, adj_close = :adj_close, volume = :volume
                    WHERE "date" = :date_val
                """)
                conn.execute(update_query, {
                    'date_val': date_to_check,
                    'open': row['open'],
                    'high': row['high'],
                    'low': row['low'],
                    'close': row['close'],
                    'adj_close': row['adj_close'],
                    'volume': volume_value
                })
            else:
                insert_query = text(f"""
                    INSERT INTO "{table_name}" (date, open, high, low, close, adj_close, volume)
                    VALUES (:date_val, :open, :high, :low, :close, :adj_close, :volume)
                """)
                conn.execute(insert_query, {
                    'date_val': date_to_check,
                    'open': row['open'],
                    'high': row['high'],
                    'low': row['low'],
                    'close': row['close'],
                    'adj_close': row['adj_close'],
                    'volume': volume_value
                })

        conn.commit()

    print(f"Données chargées avec succès pour {ticker} dans la table {table_name}.")


# --- Partie principale du script ---
if __name__ == "__main__":
    ticker_to_process = "^FCHI"
    table_name_to_use = "cac"

    create_table_if_not_exists(table_name_to_use)
    last_known_date = get_last_date(table_name_to_use)
    load_data_to_db(ticker_to_process, table_name_to_use, start_date=last_known_date)