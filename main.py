from database import get_db_engine, create_table_if_not_exists, get_last_date
from data_loader import load_data_to_db
import datetime


def main() -> None:
    """
    Fonction principale pour orchestrer le téléchargement et le chargement des données.
    """
    ticker_to_process = "^FCHI"

    table_intervals = {
        'cac_daily': '1d',
        'cac_hourly': '1h',
        'cac_weekly': '1wk'
    }

    try:
        engine = get_db_engine()
        print("Connexion à la base de données réussie.")

        for table_name, interval in table_intervals.items():

            print("\n" + "=" * 50)
            print(f"Traitement pour l'intervalle : {interval} (Table : {table_name})")
            print("=" * 50)

            create_table_if_not_exists(engine, table_name)
            last_known_date = get_last_date(engine, table_name)

            current_time = datetime.datetime.now()

            # --- DÉBUT DE LA LOGIQUE D'OPTIMISATION AMÉLIORÉE ---
            if last_known_date:
                # Pour les données horaires (1h)
                if interval == '1h':
                    if (current_time - last_known_date).total_seconds() < 3540:  # 59 minutes en secondes
                        print(
                            f"Moins d'une heure s'est écoulée depuis la dernière mise à jour "
                            f"({last_known_date.strftime('%H:%M:%S')})."
                        )
                        print("Chargement des données ignoré pour cet intervalle.")
                        continue

                # Pour les données hebdomadaires (1wk)
                elif interval == '1wk':
                    # Si la dernière date connue est dans la même semaine que la date actuelle...
                    if last_known_date.isocalendar()[1] == current_time.isocalendar()[1] and \
                            last_known_date.year == current_time.year:

                        # ... on vérifie si c'est vendredi après 18h
                        if current_time.weekday() == 4 and current_time.hour >= 18:
                            print(f"Il est 18h le vendredi. Les données de la semaine sont probablement consolidées.")
                            # On ne fait pas de 'continue' pour laisser le script charger les données
                        else:
                            # ... sinon, on ne charge rien (du lundi au jeudi, ou le vendredi avant 18h)
                            print(
                                f"La dernière mise à jour ({last_known_date.date()}) "
                                f"est dans la même semaine que la date actuelle."
                            )
                            print("Chargement des données ignoré pour cet intervalle.")
                            continue
            # --- FIN DE LA LOGIQUE D'OPTIMISATION AMÉLIORÉE ---

            load_data_to_db(engine, ticker_to_process, table_name,
                            start_date=last_known_date, interval=interval)

    except ValueError as e:
        print(f"Erreur de configuration : {e}")
    except Exception as e:
        print(f"Une erreur inattendue est survenue : {e}")


if __name__ == "__main__":
    main()
