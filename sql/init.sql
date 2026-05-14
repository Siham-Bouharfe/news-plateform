-- ============================================================
-- Init script : crée la base newsdb et toutes les tables
-- Exécuté automatiquement au premier démarrage de PostgreSQL
-- ============================================================

-- Créer la base et l'utilisateur news
CREATE USER news WITH PASSWORD 'news123';
CREATE DATABASE newsdb OWNER news;
GRANT ALL PRIVILEGES ON DATABASE newsdb TO news;

\connect newsdb news

-- ─── Table principale : articles ──────────────────────────────────────────
CREATE TABLE IF NOT EXISTS articles (
    id              SERIAL PRIMARY KEY,
    title           TEXT NOT NULL,
    author          TEXT,
    published_at    TIMESTAMP,
    category        TEXT,
    content         TEXT,
    source          TEXT NOT NULL,
    url             TEXT UNIQUE NOT NULL,
    language        VARCHAR(10),
    word_count      INTEGER,
    is_duplicate    BOOLEAN DEFAULT FALSE,
    quality_score   FLOAT DEFAULT 1.0,
    scraped_at      TIMESTAMP DEFAULT NOW(),
    layer           VARCHAR(10) DEFAULT 'silver'   -- bronze / silver / gold
);

-- ─── Table analytique : tendances par jour ────────────────────────────────
CREATE TABLE IF NOT EXISTS daily_trends (
    id              SERIAL PRIMARY KEY,
    trend_date      DATE NOT NULL,
    keyword         TEXT NOT NULL,
    occurrence      INTEGER DEFAULT 1,
    source          TEXT,
    category        TEXT,
    created_at      TIMESTAMP DEFAULT NOW(),
    UNIQUE (trend_date, keyword, source)
);

-- ─── Table analytique : volumes par source ────────────────────────────────
CREATE TABLE IF NOT EXISTS articles_by_source (
    id              SERIAL PRIMARY KEY,
    report_date     DATE NOT NULL,
    source          TEXT NOT NULL,
    article_count   INTEGER DEFAULT 0,
    avg_word_count  FLOAT,
    categories      TEXT[],
    created_at      TIMESTAMP DEFAULT NOW(),
    UNIQUE (report_date, source)
);

-- ─── Table analytique : articles par thème ────────────────────────────────
CREATE TABLE IF NOT EXISTS articles_by_category (
    id              SERIAL PRIMARY KEY,
    report_date     DATE NOT NULL,
    category        TEXT NOT NULL,
    article_count   INTEGER DEFAULT 0,
    top_keywords    TEXT[],
    created_at      TIMESTAMP DEFAULT NOW(),
    UNIQUE (report_date, category)
);

-- ─── Table qualité des données ────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS data_quality_log (
    id              SERIAL PRIMARY KEY,
    check_date      TIMESTAMP DEFAULT NOW(),
    pipeline_run    TEXT,
    total_checked   INTEGER,
    missing_title   INTEGER DEFAULT 0,
    missing_date    INTEGER DEFAULT 0,
    too_short       INTEGER DEFAULT 0,
    duplicates      INTEGER DEFAULT 0,
    passed          INTEGER,
    quality_rate    FLOAT
);

-- ─── Index pour les requêtes Grafana ──────────────────────────────────────
CREATE INDEX idx_articles_published  ON articles (published_at);
CREATE INDEX idx_articles_source     ON articles (source);
CREATE INDEX idx_articles_category   ON articles (category);
CREATE INDEX idx_trends_date         ON daily_trends (trend_date);
CREATE INDEX idx_source_date         ON articles_by_source (report_date);

-- ─── Vue pour Grafana : articles par jour ─────────────────────────────────
CREATE OR REPLACE VIEW v_articles_per_day AS
SELECT
    DATE(published_at)  AS day,
    source,
    COUNT(*)            AS total,
    AVG(word_count)     AS avg_words
FROM articles
WHERE published_at IS NOT NULL
GROUP BY DATE(published_at), source
ORDER BY day DESC;

-- ─── Vue pour Grafana : top keywords ──────────────────────────────────────
CREATE OR REPLACE VIEW v_top_keywords AS
SELECT
    keyword,
    SUM(occurrence) AS total,
    trend_date
FROM daily_trends
GROUP BY keyword, trend_date
ORDER BY total DESC;

GRANT ALL ON ALL TABLES IN SCHEMA public TO news;
GRANT ALL ON ALL SEQUENCES IN SCHEMA public TO news;
