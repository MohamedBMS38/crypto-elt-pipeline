"""
Module pour charger les données Parquet dans PostgreSQL.

Ce script lit les fichiers Parquet et les insère dans la base de données.
"""

import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine
from loguru import logger


# =============================================================================
# CONFIGURATION
# =============================================================================

# Paramètres de connexion PostgreSQL
# En production, ces valeurs viendraient de variables d'environnement
DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "crypto_db",
    "user": "crypto_user",
    "password": "crypto_pass",
}

# Construction de l'URL de connexion SQLAlchemy
# Format: postgresql://user:password@host:port/database
DATABASE_URL = (
    f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
    f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
)

# Chemin vers les données
DATA_RAW_DIR = Path(__file__).parent.parent.parent / "data" / "raw"


# =============================================================================
# FONCTIONS
# =============================================================================

def get_db_engine():
    """
    Crée et retourne une connexion à la base de données.

    Returns:
        SQLAlchemy Engine pour exécuter des requêtes
    """
    engine = create_engine(DATABASE_URL)
    return engine


def load_parquet_to_postgres(parquet_path: Path, table_name: str = "crypto_prices", schema: str = "raw") -> int:
    """
    Charge un fichier Parquet dans PostgreSQL.

    Args:
        parquet_path: Chemin vers le fichier Parquet
        table_name: Nom de la table destination
        schema: Schema PostgreSQL (raw, clean, etc.)

    Returns:
        Nombre de lignes insérées
    """
    logger.info(f"Chargement de {parquet_path} vers {schema}.{table_name}...")

    # Lire le fichier Parquet
    df = pd.read_parquet(parquet_path)
    logger.info(f"Fichier lu: {len(df)} lignes")

    # Créer la connexion
    engine = get_db_engine()

    # Charger dans PostgreSQL
    # - if_exists="append" : ajoute aux données existantes
    # - index=False : ne pas créer de colonne index
    rows_inserted = df.to_sql(
        name=table_name,
        con=engine,
        schema=schema,
        if_exists="append",
        index=False,
        method="multi",  # Insert par batch (plus rapide)
    )

    logger.success(f"{len(df)} lignes insérées dans {schema}.{table_name}")

    return len(df)


def load_all_parquet_files(date_folder: str = None) -> int:
    """
    Charge tous les fichiers Parquet d'un dossier date.

    Args:
        date_folder: Dossier date (ex: "2026-01-08"). Si None, prend le plus récent.

    Returns:
        Nombre total de lignes insérées
    """
    # Trouver le dossier
    if date_folder:
        folder = DATA_RAW_DIR / date_folder
    else:
        # Prendre le dossier le plus récent
        folders = sorted(DATA_RAW_DIR.glob("20*"))  # Dossiers commençant par 20
        if not folders:
            logger.error("Aucun dossier de données trouvé!")
            return 0
        folder = folders[-1]  # Le plus récent

    logger.info(f"Chargement depuis: {folder}")

    # Chercher le fichier combiné (all_cryptos_prices.parquet)
    combined_file = folder / "all_cryptos_prices.parquet"

    if combined_file.exists():
        return load_parquet_to_postgres(combined_file)
    else:
        # Sinon, charger tous les fichiers individuels
        total = 0
        for parquet_file in folder.glob("*.parquet"):
            total += load_parquet_to_postgres(parquet_file)
        return total


# =============================================================================
# POINT D'ENTREE
# =============================================================================

if __name__ == "__main__":
    """
    Test du chargement.
    """
    logger.info("=== Test de chargement Parquet -> PostgreSQL ===")

    # Charger les données du dossier le plus récent
    rows = load_all_parquet_files()

    print(f"\nTotal: {rows} lignes chargées")
