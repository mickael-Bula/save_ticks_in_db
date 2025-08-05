from database import get_db_engine, create_table_if_not_exists, get_last_date
from data_loader import load_data_to_db


def main() -> None:
    """
    Fonction principale pour orchestrer le téléchargement et le chargement des données.
    """
    ticker_to_process = "^FCHI"
    table_name_to_use = "cac"

    try:
        # 1. Connexion à la base de données
        engine = get_db_engine()
        print("Connexion à la base de données réussie.")

        # 2. Création de la table si elle n'existe pas
        create_table_if_not_exists(engine, table_name_to_use)
        print(f"Table '{table_name_to_use}' vérifiée.")

        # 3. Récupération de la dernière date de l'historique
        last_known_date = get_last_date(engine, table_name_to_use)

        # 4. Chargement des nouvelles données
        load_data_to_db(engine, ticker_to_process, table_name_to_use, start_date=last_known_date)

    except ValueError as e:
        print(f"Erreur de configuration : {e}")
    except Exception as e:
        print(f"Une erreur inattendue est survenue : {e}")


if __name__ == "__main__":
    main()
