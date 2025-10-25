import yfinance as yf
import pandas as pd
import datetime
from sqlalchemy import text
from sqlalchemy.engine import Engine


def load_data_to_db(engine: Engine,
                    ticker: str,
                    table_name: str,
                    start_date: datetime.datetime | None = None,
                    interval: str = '1d') -> None:
    """
    Récupère les données depuis yfinance et les charge dans la base de données.
    Prend en charge différents horizons de temps (intervalle).
    """
    print(f"Chargement des données pour {ticker} avec l'intervalle {interval}...")

    # Le paramètre 'interval' est passé à yf.download
    if start_date:
        print(f"Récupération des données depuis le {start_date.strftime('%Y-%m-%d')} avec l'intervalle {interval}")
        data = yf.download(ticker, start=start_date, interval=interval, auto_adjust=False)
    else:
        print(f"Première récupération : chargement de l'historique complet pour l'intervalle {interval}.")
        data = yf.download(ticker, period="max", interval=interval, auto_adjust=False)

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

    # Note : La logique suivante pour le marché ouvert est optimisée pour l'intervalle '1d'.
    # Pour '1h' ou '1wk', elle pourrait nécessiter une adaptation plus fine.
    current_time = datetime.datetime.now()
    is_weekday = 0 <= current_time.weekday() <= 4
    is_before_market_close = current_time.hour < 18

    if interval == '1d' and is_weekday and is_before_market_close:
        if not data.empty and data['date'].iloc[-1].date() == current_time.date():
            print("Le marché est ouvert en semaine. La dernière ligne de données (non finale) ne sera pas insérée.")
            data = data.iloc[:-1]
            if data.empty:
                print("Le DataFrame est vide après exclusion de la dernière ligne. Fin du script.")
                return

    with engine.connect() as conn:
        for _, row in data.iterrows():
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
