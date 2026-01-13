"""
DAG Airflow pour le pipeline crypto complet.

Ce DAG orchestre le pipeline ELT :
1. Extraction des données CoinGecko -> Parquet
2. Chargement des données Parquet -> PostgreSQL
3. Transformation Spark (OHLC, moyennes mobiles, variations)

Schedule: Quotidien à 6h du matin
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator


# =============================================================================
# CONFIGURATION DU DAG
# =============================================================================

default_args = {
    'owner': 'crypto_pipeline',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


# =============================================================================
# FONCTIONS PYTHON POUR LES TÂCHES
# =============================================================================

def extract_crypto_data():
    """
    Tâche 1: Extraire les données CoinGecko et sauvegarder en Parquet.

    Configuration via variables d'environnement :
    - CRYPTO_NB_CRYPTOS : Nombre de cryptos (défaut: 5)
    - CRYPTO_HISTORY_DAYS : Jours d'historique (défaut: 7)
    """
    import os
    import sys
    sys.path.insert(0, '/opt/airflow')

    from src.extraction.extract_crypto_data import extract_all_cryptos

    nb_cryptos = int(os.getenv('CRYPTO_NB_CRYPTOS', 5))
    history_days = int(os.getenv('CRYPTO_HISTORY_DAYS', 7))

    print(f"Configuration: {nb_cryptos} cryptos, {history_days} jours d'historique")

    files = extract_all_cryptos(nb_cryptos=nb_cryptos, days=history_days)

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


def transform_with_spark():
    """
    Tâche 3: Transformer les données avec Spark.

    Transformations appliquées :
    - Agrégation journalière (OHLC)
    - Moyennes mobiles (7j, 30j)
    - Variations de prix (1j, 7j)
    - Volatilité (7j)

    Les données transformées sont sauvegardées dans data/processed/
    """
    import sys
    sys.path.insert(0, '/opt/airflow')

    from src.processing.transform_prices import run_transformations

    print("Lancement des transformations Spark...")

    files = run_transformations()

    print(f"Transformations terminées. Fichiers créés: {files}")
    return files


# =============================================================================
# DÉFINITION DU DAG
# =============================================================================

with DAG(
    dag_id='crypto_extraction_pipeline',
    default_args=default_args,
    description='Pipeline ELT crypto: CoinGecko -> PostgreSQL -> Spark',
    schedule_interval='0 6 * * *',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['crypto', 'extraction', 'etl', 'spark'],
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
    # TÂCHE 3: Transformations Spark
    # =========================================================================
    task_transform = PythonOperator(
        task_id='transform_with_spark',
        python_callable=transform_with_spark,
    )

    # =========================================================================
    # ORDRE D'EXÉCUTION
    # =========================================================================
    # Extract -> Load -> Transform
    task_extract >> task_load >> task_transform
