# Crypto ELT Pipeline

Pipeline ELT pour l'analyse de données de cryptomonnaies utilisant l'API CoinGecko.

## Stack Technique

- **Orchestration**: Apache Airflow
- **Processing**: Apache Spark + dbt
- **Stockage**: PostgreSQL + Parquet
- **Dashboard**: Apache Superset
- **ML**: Jupyter + MLflow
- **Containerisation**: Docker + Docker Compose

## Structure du Projet

```
crypto-elt-pipeline/
├── dags/                  # Airflow DAGs
├── src/
│   ├── extraction/        # Scripts d'extraction CoinGecko
│   ├── processing/        # Jobs Spark
│   └── ml/                # Scripts ML
├── dbt/                   # Projet dbt
├── docker/                # Dockerfiles
├── data/
│   ├── raw/               # Fichiers Parquet bruts
│   └── processed/         # Données transformées
├── notebooks/             # Jupyter notebooks
├── tests/                 # Tests
└── docs/                  # Documentation
```

## Installation

```bash
# Créer environnement virtuel
python -m venv venv

# Activer (Windows)
.\venv\Scripts\activate

# Installer dépendances
pip install -r requirements.txt
```

## Démarrage

```bash
docker-compose up -d
```

## Licence

MIT
