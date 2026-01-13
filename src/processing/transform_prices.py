"""
Transformations Spark pour les données crypto.

Ce module contient les transformations appliquées aux données brutes :
1. Agrégation journalière (données horaires → 1 ligne par jour)
2. Calcul de métriques (moyennes mobiles, variations %)
3. Nettoyage (nulls, doublons)

Logique:
    Données horaires → Agrégation OHLC journalière → Métriques sur données journalières

Pourquoi cet ordre ?
    Les données CoinGecko sont horaires. Si on calcule une "moyenne 7 jours"
    directement sur les données horaires avec rowsBetween(-6, 0), on obtient
    une moyenne sur 7 HEURES, pas 7 jours.

    En agrégeant d'abord en journalier (1 ligne = 1 jour), les fenêtres
    glissantes fonctionnent correctement.

Usage:
    python -m src.processing.transform_prices
"""

import os
from datetime import datetime
from pathlib import Path

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from loguru import logger

from src.processing.spark_config import get_spark_session, stop_spark_session


# =============================================================================
# CONFIGURATION
# =============================================================================

RAW_DATA_PATH = "data/raw"
PROCESSED_DATA_PATH = "data/processed"


# =============================================================================
# FONCTIONS DE CHARGEMENT
# =============================================================================

def load_raw_data(spark: SparkSession, date: str = None) -> DataFrame:
    """
    Charge les données brutes depuis les fichiers Parquet.

    Args:
        spark: SparkSession active
        date: Date spécifique (YYYY-MM-DD) ou None pour le plus récent

    Returns:
        DataFrame Spark avec les données brutes
    """
    if date:
        path = f"{RAW_DATA_PATH}/{date}/all_cryptos_prices.parquet"
    else:
        # Trouver le dossier le plus récent
        raw_path = Path(RAW_DATA_PATH)
        dates = sorted([d.name for d in raw_path.iterdir() if d.is_dir()])
        if not dates:
            raise FileNotFoundError(f"Aucune donnée dans {RAW_DATA_PATH}")
        latest_date = dates[-1]
        path = f"{RAW_DATA_PATH}/{latest_date}/all_cryptos_prices.parquet"
        logger.info(f"Chargement des données du {latest_date}")

    logger.info(f"Lecture de: {path}")
    df = spark.read.parquet(path)

    row_count = df.count()
    logger.info(f"Données chargées: {row_count} lignes")

    return df


# =============================================================================
# FONCTIONS DE TRANSFORMATION
# =============================================================================

def clean_data(df: DataFrame) -> DataFrame:
    """
    Nettoie les données brutes.

    Opérations:
    - Suppression des lignes avec valeurs nulles sur colonnes critiques
    - Suppression des doublons (même crypto + même timestamp)
    - Filtrage des prix invalides (<=0)

    Args:
        df: DataFrame brut

    Returns:
        DataFrame nettoyé
    """
    logger.info("Nettoyage des données...")

    initial_count = df.count()

    df_clean = (
        df
        # Supprimer les nulls sur colonnes critiques
        .dropna(subset=["crypto_id", "timestamp", "price"])
        # Supprimer les doublons
        .dropDuplicates(["crypto_id", "timestamp"])
        # Filtrer les prix invalides
        .filter(F.col("price") > 0)
    )

    final_count = df_clean.count()
    removed = initial_count - final_count

    logger.info(f"Nettoyage: {removed} lignes supprimées ({final_count} restantes)")

    return df_clean


def create_daily_ohlc(df: DataFrame) -> DataFrame:
    """
    Agrège les données horaires en données journalières OHLC.

    OHLC = Open, High, Low, Close (standard financier)

    Après cette transformation: 1 ligne = 1 jour par crypto
    Ce qui permet d'utiliser correctement les fenêtres glissantes.

    Args:
        df: DataFrame avec données horaires

    Returns:
        DataFrame avec 1 ligne par jour par crypto
    """
    logger.info("Agrégation journalière (OHLC)...")

    # Extraire la date du timestamp
    # Le timestamp est en millisecondes (epoch), on le convertit en date
    # On utilise la colonne "datetime" qui est déjà formatée
    df_with_date = df.withColumn("date", F.to_date("datetime"))

    df_daily = (
        df_with_date
        .groupBy("crypto_id", "date")
        .agg(
            # Prix d'ouverture (premier du jour)
            F.first("price").alias("open"),
            # Prix le plus haut du jour
            F.max("price").alias("high"),
            # Prix le plus bas du jour
            F.min("price").alias("low"),
            # Prix de clôture (dernier du jour)
            F.last("price").alias("close"),
            # Volume total du jour
            F.sum("volume").alias("volume"),
            # Market cap moyen du jour
            F.round(F.avg("market_cap"), 0).alias("market_cap"),
            # Nombre de points de données (pour vérification)
            F.count("*").alias("data_points"),
        )
        .orderBy("crypto_id", "date")
    )

    row_count = df_daily.count()
    logger.info(f"Agrégation terminée: {row_count} lignes (1 par jour par crypto)")

    return df_daily


def add_moving_averages(df: DataFrame) -> DataFrame:
    """
    Ajoute les moyennes mobiles sur les données journalières.

    Maintenant que 1 ligne = 1 jour, rowsBetween(-6, 0) = vraiment 7 jours.

    Moyennes calculées:
    - MA 7 jours (court terme, ~1 semaine)
    - MA 30 jours (moyen terme, ~1 mois)

    Args:
        df: DataFrame journalier

    Returns:
        DataFrame avec colonnes ma_7d et ma_30d
    """
    logger.info("Calcul des moyennes mobiles...")

    # Fenêtre par crypto, ordonnée par date
    # Maintenant: 1 ligne = 1 jour, donc rowsBetween(-6, 0) = 7 jours
    window_7d = (
        Window
        .partitionBy("crypto_id")
        .orderBy("date")
        .rowsBetween(-6, 0)  # 7 derniers jours
    )

    window_30d = (
        Window
        .partitionBy("crypto_id")
        .orderBy("date")
        .rowsBetween(-29, 0)  # 30 derniers jours
    )

    df_with_ma = (
        df
        .withColumn("ma_7d", F.round(F.avg("close").over(window_7d), 2))
        .withColumn("ma_30d", F.round(F.avg("close").over(window_30d), 2))
    )

    logger.info("Moyennes mobiles ajoutées (ma_7d, ma_30d)")

    return df_with_ma


def add_price_changes(df: DataFrame) -> DataFrame:
    """
    Ajoute les variations de prix sur les données journalières.

    Maintenant que 1 ligne = 1 jour, lag(1) = vraiment hier.

    Variations calculées:
    - Variation vs veille (%)
    - Variation vs 7 jours (%)

    Args:
        df: DataFrame journalier

    Returns:
        DataFrame avec colonnes change_1d_pct et change_7d_pct
    """
    logger.info("Calcul des variations de prix...")

    window = Window.partitionBy("crypto_id").orderBy("date")

    df_with_changes = (
        df
        # Prix de la veille (lag 1 = hier)
        .withColumn("prev_close", F.lag("close", 1).over(window))
        # Prix d'il y a 7 jours
        .withColumn("close_7d_ago", F.lag("close", 7).over(window))
        # Variation journalière en %
        .withColumn(
            "change_1d_pct",
            F.round(
                ((F.col("close") - F.col("prev_close")) / F.col("prev_close")) * 100,
                2
            )
        )
        # Variation 7 jours en %
        .withColumn(
            "change_7d_pct",
            F.round(
                ((F.col("close") - F.col("close_7d_ago")) / F.col("close_7d_ago")) * 100,
                2
            )
        )
        # Supprimer colonnes temporaires
        .drop("prev_close", "close_7d_ago")
    )

    logger.info("Variations ajoutées (change_1d_pct, change_7d_pct)")

    return df_with_changes


def add_volatility(df: DataFrame) -> DataFrame:
    """
    Ajoute la volatilité (écart-type sur 7 jours).

    La volatilité mesure l'amplitude des variations de prix.
    Plus elle est élevée, plus le prix fluctue.

    Args:
        df: DataFrame journalier

    Returns:
        DataFrame avec colonne volatility_7d
    """
    logger.info("Calcul de la volatilité...")

    window_7d = (
        Window
        .partitionBy("crypto_id")
        .orderBy("date")
        .rowsBetween(-6, 0)
    )

    df_with_vol = df.withColumn(
        "volatility_7d",
        F.round(F.stddev("close").over(window_7d), 2)
    )

    logger.info("Volatilité ajoutée (volatility_7d)")

    return df_with_vol


# =============================================================================
# FONCTION DE SAUVEGARDE
# =============================================================================

def save_processed_data(df: DataFrame, name: str) -> str:
    """
    Sauvegarde un DataFrame transformé en Parquet.

    Args:
        df: DataFrame à sauvegarder
        name: Nom du fichier (sans extension)

    Returns:
        Chemin du fichier créé
    """
    today = datetime.now().strftime("%Y-%m-%d")
    output_dir = Path(PROCESSED_DATA_PATH) / today
    output_dir.mkdir(parents=True, exist_ok=True)

    output_path = str(output_dir / f"{name}.parquet")

    logger.info(f"Sauvegarde: {output_path}")
    df.write.mode("overwrite").parquet(output_path)
    logger.success(f"Fichier sauvegardé: {output_path}")

    return output_path


# =============================================================================
# FONCTION PRINCIPALE
# =============================================================================

def run_transformations() -> dict:
    """
    Exécute toutes les transformations sur les données brutes.

    Pipeline:
        1. Charger données brutes (horaires)
        2. Nettoyer (nulls, doublons, prix invalides)
        3. Agréger en journalier (OHLC)
        4. Ajouter moyennes mobiles (7j, 30j)
        5. Ajouter variations (1j, 7j)
        6. Ajouter volatilité (7j)
        7. Sauvegarder

    Returns:
        Dictionnaire avec les chemins des fichiers créés
    """
    logger.info("=" * 60)
    logger.info("DEBUT DES TRANSFORMATIONS SPARK")
    logger.info("=" * 60)

    spark = get_spark_session("CryptoTransform")

    try:
        # 1. Charger les données brutes (horaires)
        df_raw = load_raw_data(spark)

        logger.info("Aperçu des données brutes:")
        df_raw.show(5, truncate=False)

        # 2. Nettoyer
        df_clean = clean_data(df_raw)

        # 3. Agréger en journalier (OHLC)
        # IMPORTANT: Après ça, 1 ligne = 1 jour
        df_daily = create_daily_ohlc(df_clean)

        # 4. Ajouter les métriques (sur données journalières)
        df_with_ma = add_moving_averages(df_daily)
        df_with_changes = add_price_changes(df_with_ma)
        df_final = add_volatility(df_with_changes)

        # 5. Sauvegarder
        files = {}
        files["daily_prices"] = save_processed_data(df_final, "crypto_daily_prices")

        logger.info("=" * 60)
        logger.success("TRANSFORMATIONS TERMINEES")
        logger.info("=" * 60)

        # Afficher un aperçu du résultat
        logger.info("Aperçu des données transformées:")
        df_final.select(
            "crypto_id", "date", "close",
            "ma_7d", "change_1d_pct", "volatility_7d"
        ).show(15)

        return files

    finally:
        stop_spark_session(spark)


# =============================================================================
# POINT D'ENTREE
# =============================================================================

if __name__ == "__main__":
    """
    Lancer les transformations manuellement:
        cd C:/projets/crypto-elt-pipeline
        python -m src.processing.transform_prices
    """
    files = run_transformations()

    print("\nFichiers créés:")
    for name, path in files.items():
        print(f"  - {name}: {path}")
