"""
Module de processing Spark pour le pipeline crypto.

Ce module contient:
- spark_config: Configuration de la SparkSession
- transform_prices: Transformations des données de prix
"""

from src.processing.spark_config import get_spark_session, stop_spark_session
from src.processing.transform_prices import run_transformations

__all__ = [
    "get_spark_session",
    "stop_spark_session",
    "run_transformations",
]
