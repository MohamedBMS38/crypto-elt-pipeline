"""
Configuration Spark pour le pipeline crypto.

Ce module fournit une SparkSession configurée pour notre projet.
On utilise Spark en mode "local" (pas de cluster), suffisant pour :
- Projet portfolio
- Volumes de données modérés (quelques Go)
- Démontrer les compétences Spark

Usage:
    from src.processing.spark_config import get_spark_session

    spark = get_spark_session()
    df = spark.read.parquet("data/raw/...")
"""

from pyspark.sql import SparkSession
from loguru import logger


def get_spark_session(app_name: str = "CryptoETL") -> SparkSession:
    """
    Crée et retourne une SparkSession configurée.

    Args:
        app_name: Nom de l'application Spark (visible dans l'UI Spark)

    Returns:
        SparkSession configurée pour le projet

    Exemple:
        spark = get_spark_session()
        df = spark.read.parquet("data/raw/2026-01-08/all_cryptos_prices.parquet")
        df.show()
    """
    logger.info(f"Initialisation de SparkSession: {app_name}")

    spark = (
        SparkSession.builder
        # Nom de l'application
        .appName(app_name)
        # Mode local avec tous les cores disponibles
        # local[*] = utilise tous les CPU de la machine
        # local[4] = utiliserait 4 CPU
        .master("local[*]")
        # Configuration mémoire
        # Driver = le programme principal qui orchestre
        .config("spark.driver.memory", "2g")
        # Activer le support des timestamps avec timezone
        .config("spark.sql.session.timeZone", "UTC")
        # Format de fichier par défaut
        .config("spark.sql.sources.default", "parquet")
        # Créer la session (ou récupérer si elle existe déjà)
        .getOrCreate()
    )

    # Réduire les logs Spark (sinon c'est très verbeux)
    spark.sparkContext.setLogLevel("WARN")

    logger.success("SparkSession initialisée avec succès")

    return spark


def stop_spark_session(spark: SparkSession) -> None:
    """
    Arrête proprement la SparkSession.

    Important d'appeler cette fonction à la fin du traitement
    pour libérer les ressources.

    Args:
        spark: La SparkSession à arrêter
    """
    logger.info("Arrêt de la SparkSession...")
    spark.stop()
    logger.success("SparkSession arrêtée")


# =============================================================================
# TEST RAPIDE
# =============================================================================

if __name__ == "__main__":
    """
    Test rapide pour vérifier que Spark fonctionne.

    Usage:
        cd C:/projets/crypto-elt-pipeline
        python -m src.processing.spark_config
    """
    print("Test de la configuration Spark...")

    # Créer une session
    spark = get_spark_session("TestSpark")

    # Afficher la version
    print(f"Spark version: {spark.version}")

    # Créer un petit DataFrame de test
    test_data = [
        ("bitcoin", 50000.0),
        ("ethereum", 3000.0),
        ("solana", 100.0),
    ]
    df = spark.createDataFrame(test_data, ["crypto", "price"])

    print("\nDataFrame de test:")
    df.show()

    # Arrêter proprement
    stop_spark_session(spark)

    print("\nTest terminé avec succès!")
