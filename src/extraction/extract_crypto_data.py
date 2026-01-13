"""
Script principal d'extraction des données crypto.

Ce script orchestre l'extraction complète :
1. Récupère la liste des top cryptos
2. Pour chaque crypto, récupère l'historique des prix
3. Sauvegarde le tout en fichiers Parquet

Usage:
    python -m src.extraction.extract_crypto_data

Configuration via variables d'environnement :
    CRYPTO_NB_CRYPTOS   : Nombre de cryptos à extraire (défaut: 20)
    CRYPTO_HISTORY_DAYS : Nombre de jours d'historique (défaut: 365)
"""

import os
import time
import pandas as pd
from loguru import logger

# Import de nos modules (qu'on a créés ensemble)
from src.extraction.coingecko_client import (
    get_top_cryptos,
    get_price_history,
    RATE_LIMIT_SECONDS,
)
from src.extraction.save_to_parquet import (
    history_to_dataframe,
    save_dataframe_to_parquet,
)


# =============================================================================
# CONFIGURATION (via variables d'environnement)
# =============================================================================

# Nombre de cryptos à extraire (défaut: 20 pour production)
NB_CRYPTOS = int(os.getenv('CRYPTO_NB_CRYPTOS', 20))

# Nombre de jours d'historique (défaut: 365 pour 1 an)
HISTORY_DAYS = int(os.getenv('CRYPTO_HISTORY_DAYS', 365))


# =============================================================================
# FONCTIONS
# =============================================================================

def extract_all_cryptos(nb_cryptos: int = NB_CRYPTOS, days: int = HISTORY_DAYS) -> list[str]:
    """
    Extrait les données de plusieurs cryptomonnaies et les sauvegarde en Parquet.

    Args:
        nb_cryptos: Nombre de cryptos à extraire (défaut: 20)
        days: Nombre de jours d'historique (défaut: 365)

    Returns:
        Liste des chemins des fichiers créés
    """
    logger.info(f"=== Debut extraction: {nb_cryptos} cryptos, {days} jours ===")

    # Étape 1 : Récupérer la liste des top cryptos
    cryptos = get_top_cryptos(limit=nb_cryptos)

    # Extraire juste les IDs (ex: ["bitcoin", "ethereum", ...])
    crypto_ids = [crypto["id"] for crypto in cryptos]
    logger.info(f"Cryptos a extraire: {crypto_ids}")

    # Liste pour stocker les chemins des fichiers créés
    created_files = []

    # Liste pour stocker tous les DataFrames (pour le fichier combiné)
    all_dataframes = []

    # Étape 2 : Pour chaque crypto, extraire l'historique
    for i, crypto_id in enumerate(crypto_ids):
        logger.info(f"[{i+1}/{nb_cryptos}] Extraction de {crypto_id}...")

        try:
            # Récupérer l'historique
            history = get_price_history(crypto_id, days=days)

            # Convertir en DataFrame
            df = history_to_dataframe(crypto_id, history)

            # Ajouter à la liste des DataFrames
            all_dataframes.append(df)

            # Sauvegarder en Parquet individuel
            file_path = save_dataframe_to_parquet(df, f"{crypto_id}_prices")
            created_files.append(str(file_path))

        except Exception as e:
            # Si erreur sur une crypto, log et continuer avec les autres
            logger.error(f"Erreur pour {crypto_id}: {e}")

        # Pause pour respecter le rate limit (sauf pour la dernière)
        if i < len(crypto_ids) - 1:
            logger.debug(f"Pause de {RATE_LIMIT_SECONDS} secondes (rate limiting)...")
            time.sleep(RATE_LIMIT_SECONDS)

    # Étape 3 : Créer un fichier combiné avec toutes les cryptos
    if all_dataframes:
        logger.info("Creation du fichier combine...")

        # pd.concat = fusionner plusieurs DataFrames en un seul
        combined_df = pd.concat(all_dataframes, ignore_index=True)

        combined_path = save_dataframe_to_parquet(combined_df, "all_cryptos_prices")
        created_files.append(str(combined_path))

        logger.info(f"Fichier combine: {len(combined_df)} lignes totales")

    logger.success(f"=== Extraction terminee: {len(created_files)} fichiers crees ===")

    return created_files


# =============================================================================
# POINT D'ENTREE
# =============================================================================

if __name__ == "__main__":
    """
    Ce bloc s'exécute uniquement quand on lance le script directement :
        python -m src.extraction.extract_crypto_data

    Il ne s'exécute PAS quand on importe le module :
        from src.extraction.extract_crypto_data import extract_all_cryptos

    La configuration est lue depuis les variables d'environnement :
        CRYPTO_NB_CRYPTOS   : Nombre de cryptos (défaut: 20)
        CRYPTO_HISTORY_DAYS : Jours d'historique (défaut: 365)

    Exemples:
        # Mode dev (rapide)
        set CRYPTO_NB_CRYPTOS=5 && set CRYPTO_HISTORY_DAYS=7 && python -m src.extraction.extract_crypto_data

        # Mode prod (complet)
        python -m src.extraction.extract_crypto_data
    """
    print(f"Configuration: {NB_CRYPTOS} cryptos, {HISTORY_DAYS} jours")

    files = extract_all_cryptos()  # Utilise NB_CRYPTOS et HISTORY_DAYS par défaut

    print("\nFichiers crees:")
    for f in files:
        print(f"  - {f}")
