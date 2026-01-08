"""
Module pour sauvegarder les données crypto en format Parquet.

Parquet est un format de fichier columnar optimisé pour le Big Data.
Il est compressé, typé, et très efficace pour l'analyse.
"""

import pandas as pd  # Manipulation de données tabulaires
from pathlib import Path  # Gestion des chemins de fichiers (moderne)
from datetime import datetime  # Pour les dates
from loguru import logger


# =============================================================================
# CONFIGURATION
# =============================================================================

# Dossier où sauvegarder les fichiers Parquet
DATA_RAW_DIR = Path(__file__).parent.parent.parent / "data" / "raw"


# =============================================================================
# FONCTIONS
# =============================================================================

def history_to_dataframe(crypto_id: str, history_data: dict) -> pd.DataFrame:
    """
    Convertit les données d'historique de l'API en DataFrame Pandas.

    Args:
        crypto_id: Identifiant de la crypto (ex: "bitcoin")
        history_data: Données brutes de l'API (dict avec prices, volumes, etc.)

    Returns:
        DataFrame avec colonnes: timestamp, datetime, crypto_id, price, volume, market_cap
    """
    # Extraire les listes de l'API
    prices = history_data.get("prices", [])
    volumes = history_data.get("total_volumes", [])
    market_caps = history_data.get("market_caps", [])

    # Créer une liste de dictionnaires (chaque dict = une ligne)
    rows = []

    for i, (timestamp_ms, price) in enumerate(prices):
        row = {
            "timestamp": timestamp_ms,
            # Convertir timestamp milliseconds en datetime
            "datetime": datetime.fromtimestamp(timestamp_ms / 1000),
            "crypto_id": crypto_id,
            "price": price,
            # Récupérer volume et market_cap au même index (si disponible)
            "volume": volumes[i][1] if i < len(volumes) else None,
            "market_cap": market_caps[i][1] if i < len(market_caps) else None,
        }
        rows.append(row)

    # Créer le DataFrame à partir de la liste
    df = pd.DataFrame(rows)

    logger.info(f"DataFrame créé: {len(df)} lignes, colonnes: {list(df.columns)}")

    return df


def save_dataframe_to_parquet(df: pd.DataFrame, filename: str) -> Path:
    """
    Sauvegarde un DataFrame en fichier Parquet.

    Args:
        df: Le DataFrame à sauvegarder
        filename: Nom du fichier (sans extension)

    Returns:
        Chemin complet du fichier créé

    Organisation des fichiers:
        data/raw/YYYY-MM-DD/filename.parquet
    """
    # Créer un sous-dossier avec la date du jour
    today = datetime.now().strftime("%Y-%m-%d")  # Format: 2026-01-08
    output_dir = DATA_RAW_DIR / today

    # Créer le dossier s'il n'existe pas
    output_dir.mkdir(parents=True, exist_ok=True)

    # Chemin complet du fichier
    file_path = output_dir / f"{filename}.parquet"

    # Sauvegarder en Parquet
    # - index=False : ne pas sauvegarder l'index du DataFrame
    # - compression="snappy" : compression rapide et efficace
    df.to_parquet(file_path, index=False, compression="snappy")

    # Calculer la taille du fichier
    file_size_mb = file_path.stat().st_size / (1024 * 1024)

    logger.success(f"Fichier sauvegardé: {file_path} ({file_size_mb:.2f} MB)")

    return file_path


