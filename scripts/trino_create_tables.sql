-- trino_create_tables.sql
-- Catalog: nessie (Iceberg connector configurado para Project Nessie)
-- Executar uma única vez: trino --execute "-f trino_create_tables.sql" --catalog nessie

-- ==============================================================
-- 1. Namespaces (schemas)
-- ==============================================================

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- ==============================================================
-- 2. Tabelas Bronze (estruturas mínimas)
--    OBS.: se o loader Python já criou estas tabelas, os CREATE
--    serão ignorados graças ao IF NOT EXISTS.
-- ==============================================================

CREATE TABLE IF NOT EXISTS bronze.albums (
    id          BIGINT,
    title       VARCHAR,
    band        BIGINT,
    year        INTEGER,
    genre       VARCHAR,
    created_at  TIMESTAMP
)
WITH (format = 'PARQUET');

CREATE TABLE IF NOT EXISTS bronze.bands (
    id          BIGINT,
    name        VARCHAR,
    country     VARCHAR,
    formed_in   INTEGER,
    created_at  TIMESTAMP
)
WITH (format = 'PARQUET');

CREATE TABLE IF NOT EXISTS bronze.reviews (
    id          BIGINT,
    album       BIGINT,
    reviewer    VARCHAR,
    score       DOUBLE,
    created_at  TIMESTAMP
)
WITH (format = 'PARQUET');

-- ==============================================================
-- 3. Tabelas Silver (transformadas)
-- ==============================================================

CREATE TABLE IF NOT EXISTS silver.albums (
    album_id        BIGINT,
    album_title     VARCHAR,
    band_id         BIGINT,
    year            INTEGER,
    genre           VARCHAR
)
WITH (format = 'PARQUET');

CREATE TABLE IF NOT EXISTS silver.bands (
    band_id         BIGINT,
    band_name       VARCHAR,
    country         VARCHAR,
    formed_in       INTEGER
)
WITH (format = 'PARQUET');

CREATE TABLE IF NOT EXISTS silver.reviews (
    review_id   BIGINT,
    album_id    BIGINT,
    score       DOUBLE
)
WITH (format = 'PARQUET');

CREATE TABLE IF NOT EXISTS silver.music_catalog (
    album_id        BIGINT,
    album_title     VARCHAR,
    band_id         BIGINT,
    band_name       VARCHAR,
    country         VARCHAR,
    year            INTEGER,
    genre           VARCHAR
)
WITH (format = 'PARQUET');

-- ==============================================================
-- 4. Tabelas Gold (métricas / agregações)
-- ==============================================================

CREATE TABLE IF NOT EXISTS gold.top10_by_country (
    country         VARCHAR,
    band_id         BIGINT,
    band_name       VARCHAR,
    review_count    BIGINT,
    avg_score       DOUBLE
)
WITH (format = 'PARQUET');

CREATE TABLE IF NOT EXISTS gold.band_avg_scores (
    band_id         BIGINT,
    band_name       VARCHAR,
    country         VARCHAR,
    review_count    BIGINT,
    avg_score       DOUBLE,
    min_score       DOUBLE,
    max_score       DOUBLE
)
WITH (format = 'PARQUET');

-- ==============================================================
-- 5. Views auxiliares (opcional)
-- ==============================================================

CREATE OR REPLACE VIEW gold.band_score_ranking AS
SELECT band_name,
       avg_score,
       review_count,
       country
FROM gold.band_avg_scores
ORDER BY avg_score DESC
LIMIT 100;