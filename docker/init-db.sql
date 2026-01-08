-- Script d'initialisation de la base de données
-- Exécuté automatiquement au premier démarrage de PostgreSQL

-- =============================================================================
-- SCHEMAS
-- =============================================================================

-- Schema pour les données brutes (raw)
CREATE SCHEMA IF NOT EXISTS raw;
COMMENT ON SCHEMA raw IS 'Données brutes extraites de CoinGecko';

-- Schema pour les données transformées (clean)
CREATE SCHEMA IF NOT EXISTS clean;
COMMENT ON SCHEMA clean IS 'Données nettoyées et transformées';

-- =============================================================================
-- TABLE: raw.crypto_prices
-- =============================================================================

CREATE TABLE IF NOT EXISTS raw.crypto_prices (
    id              SERIAL PRIMARY KEY,           -- ID auto-incrémenté
    timestamp       BIGINT NOT NULL,              -- Timestamp en millisecondes
    datetime        TIMESTAMP NOT NULL,           -- Date/heure lisible
    crypto_id       VARCHAR(50) NOT NULL,         -- ID de la crypto (bitcoin, ethereum...)
    price           DOUBLE PRECISION,             -- Prix en USD
    volume          DOUBLE PRECISION,             -- Volume échangé
    market_cap      DOUBLE PRECISION,             -- Capitalisation
    created_at      TIMESTAMP DEFAULT NOW()       -- Date d'insertion
);

-- Index pour accélérer les requêtes
CREATE INDEX IF NOT EXISTS idx_crypto_prices_crypto_id ON raw.crypto_prices(crypto_id);
CREATE INDEX IF NOT EXISTS idx_crypto_prices_datetime ON raw.crypto_prices(datetime);

COMMENT ON TABLE raw.crypto_prices IS 'Prix historiques des cryptomonnaies depuis CoinGecko';

-- =============================================================================
-- MESSAGE DE CONFIRMATION
-- =============================================================================

DO $$
BEGIN
    RAISE NOTICE 'Base de données initialisée avec succès!';
    RAISE NOTICE 'Schemas créés: raw, clean';
    RAISE NOTICE 'Table créée: raw.crypto_prices';
END $$;
