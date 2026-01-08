"""
Client pour l'API CoinGecko.

Ce module permet de récupérer les données de cryptomonnaies
depuis l'API gratuite de CoinGecko.
"""

import requests  # Pour faire des requêtes HTTP (comme curl)
import time      # Pour gérer les pauses entre requêtes (rate limiting)
from loguru import logger  # Pour afficher des logs propres


# =============================================================================
# CONFIGURATION
# =============================================================================

# URL de base de l'API CoinGecko (version gratuite)
BASE_URL = "https://api.coingecko.com/api/v3"

# Limite de l'API gratuite : 30 requêtes par minute
# On attend 2 secondes entre chaque requête pour être safe (30 req/min = 1 req/2sec)
RATE_LIMIT_SECONDS = 2

# Nombre de tentatives en cas d'erreur
MAX_RETRIES = 3



# =============================================================================
# FONCTIONS
# =============================================================================

def get_top_cryptos(limit: int = 20) -> list[dict]:
    """
    Récupère la liste des top cryptomonnaies par capitalisation.

    Args:
        limit: Nombre de cryptos à récupérer (défaut: 20)

    Returns:
        Liste de dictionnaires avec les infos de chaque crypto

    Exemple de retour:
        [
            {"id": "bitcoin", "symbol": "btc", "current_price": 89000, ...},
            {"id": "ethereum", "symbol": "eth", "current_price": 3000, ...},
        ]
    """
    # Construction de l'URL avec les paramètres
    url = f"{BASE_URL}/coins/markets"

    # Paramètres de la requête (ce qu'on a testé avec curl)
    params = {
        "vs_currency": "usd",           # Prix en dollars
        "order": "market_cap_desc",     # Trié par market cap décroissant
        "per_page": limit,              # Nombre de résultats
        "page": 1,                      # Page 1
        "sparkline": False,             # Pas besoin des mini-graphiques
    }

    logger.info(f"Récupération du top {limit} des cryptomonnaies...")

    # Faire la requête HTTP GET
    response = requests.get(url, params=params, timeout=30)

    # Vérifier que la requête a réussi (status code 200 = OK)
    response.raise_for_status()

    # Convertir la réponse JSON en liste Python
    cryptos = response.json()

    logger.success(f"{len(cryptos)} cryptos récupérées")

    return cryptos


def get_price_history(crypto_id: str, days: int = 365) -> dict:
    """
    Récupère l'historique des prix d'une cryptomonnaie.

    Args:
        crypto_id: Identifiant de la crypto (ex: "bitcoin", "ethereum")
        days: Nombre de jours d'historique (défaut: 365 = 1 an)

    Returns:
        Dictionnaire avec prices, market_caps, total_volumes

    Note sur la granularité (définie par CoinGecko):
        - 1 jour : données toutes les 5 minutes
        - 2-90 jours : données horaires
        - 91+ jours : données journalières
    """
    url = f"{BASE_URL}/coins/{crypto_id}/market_chart"

    params = {
        "vs_currency": "usd",
        "days": days,
    }

    logger.info(f"Récupération historique {days} jours pour {crypto_id}...")

    # Gestion des erreurs avec retry
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(url, params=params, timeout=60)
            response.raise_for_status()

            data = response.json()

            # Compter le nombre de points de données
            nb_prices = len(data.get("prices", []))
            logger.success(f"{crypto_id}: {nb_prices} points de prix récupérés")

            return data

        except requests.exceptions.RequestException as e:
            # Si erreur, attendre et réessayer
            logger.warning(f"Tentative {attempt + 1}/{MAX_RETRIES} échouée: {e}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(RATE_LIMIT_SECONDS * 2)  # Attendre plus longtemps
            else:
                raise  # Si toutes les tentatives échouent, propager l'erreur


