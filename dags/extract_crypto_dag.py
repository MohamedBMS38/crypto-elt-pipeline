"""
DAG Airflow pour l'extraction des données crypto.

Ce DAG orchestre le pipeline complet :
1. Extraction des données CoinGecko -> Parquet
2. Chargement des données Parquet -> PostgreSQL

Schedule: Quotidien à 6h du matin
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator


# =============================================================================
# CONFIGURATION DU DAG
# =============================================================================

# Arguments par défaut pour toutes les tâches
default_args = {
    'owner': 'crypto_pipeline',           # Propriétaire du DAG
    'depends_on_past': False,             # Ne dépend pas des runs précédents
    'email_on_failure': False,            # Pas d'email en cas d'échec
    'email_on_retry': False,              # Pas d'email en cas de retry
    'retries': 1,                         # Nombre de tentatives en cas d'échec
    'retry_delay': timedelta(minutes=5),  # Délai entre les tentatives
}


# =============================================================================
# FONCTIONS PYTHON POUR LES TÂCHES
# =============================================================================

def extract_crypto_data():
    """
    Tâche 1: Extraire les données CoinGecko et sauvegarder en Parquet.
    """
    import sys
    sys.path.insert(0, '/opt/airflow')

    from src.extraction.extract_crypto_data import extract_all_cryptos

    # Extraire 5 cryptos sur 7 jours (pour les tests)
    # En production: extract_all_cryptos(nb_cryptos=20, days=365)
    files = extract_all_cryptos(nb_cryptos=5, days=7)

    print(f"Fichiers créés: {files}")
    return files


def load_to_postgres():
    """
    Tâche 2: Charger les données Parquet dans PostgreSQL.
    """
    import sys
    sys.path.insert(0, '/opt/airflow')

    from src.extraction.load_to_postgres import load_all_parquet_files

    rows = load_all_parquet_files()

    print(f"Lignes chargées: {rows}")
    return rows


# =============================================================================
# DÉFINITION DU DAG
# =============================================================================

with DAG(
    dag_id='crypto_extraction_pipeline',      # Identifiant unique du DAG
    default_args=default_args,
    description='Pipeline extraction crypto CoinGecko -> PostgreSQL',
    schedule_interval='0 6 * * *',            # Cron: tous les jours à 6h
    start_date=datetime(2026, 1, 1),          # Date de début
    catchup=False,                            # Ne pas exécuter les runs passés
    tags=['crypto', 'extraction', 'etl'],     # Tags pour filtrer dans l'UI
) as dag:

    # =========================================================================
    # TÂCHE 1: Extraction CoinGecko -> Parquet
    # =========================================================================
    task_extract = PythonOperator(
        task_id='extract_crypto_data',
        python_callable=extract_crypto_data,
    )

    # =========================================================================
    # TÂCHE 2: Chargement Parquet -> PostgreSQL
    # =========================================================================
    task_load = PythonOperator(
        task_id='load_to_postgres',
        python_callable=load_to_postgres,
    )

    # =========================================================================
    # DÉFINITION DE L'ORDRE D'EXÉCUTION
    # =========================================================================
    # task_extract doit finir AVANT task_load
    task_extract >> task_load
