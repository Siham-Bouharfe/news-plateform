# News Platform — Architecture Big Data

Plateforme de collecte et d'analyse d'articles de presse (BBC, Al Jazeera).

## Stack technique
| Composant | Rôle |
|-----------|------|
| Python + BeautifulSoup | Scraping web |
| Apache Kafka | Streaming ingestion |
| MinIO | Data Lake (Bronze/Silver/Gold) |
| Apache Airflow | Orchestration des pipelines |
| PostgreSQL | Data Warehouse |
| Grafana | Visualisation & dashboards |

## Architecture Médaillon
```
[BBC / Al Jazeera]
        │
        ▼ Scraping (Python)
  ┌─────────────┐
  │   BRONZE    │  ← Articles bruts (JSON) dans MinIO
  └──────┬──────┘
         │ Nettoyage, normalisation, détection langue
  ┌──────▼──────┐
  │   SILVER    │  ← Articles nettoyés dans MinIO
  └──────┬──────┘
         │ Agrégations, tendances, stats
  ┌──────▼──────┐
  │    GOLD     │  ← Tables analytiques dans PostgreSQL
  └──────┬──────┘
         │
  ┌──────▼──────┐
  │   GRAFANA   │  ← Dashboards
  └─────────────┘
```

## Démarrage rapide

### 1. Prérequis
- Docker Desktop installé et lancé
- 8 Go RAM minimum

### 2. Lancer la plateforme
```bash
# Cloner / placer les fichiers du projet
cd news-platform

# Démarrer tous les services
docker-compose up -d

# Vérifier que tout tourne
docker-compose ps
```

### 3. Accès aux interfaces
| Service | URL | Login |
|---------|-----|-------|
| Airflow | http://localhost:8080 | admin / admin |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |
| Grafana | http://localhost:3000 | admin / admin |
| PostgreSQL | localhost:5432 | news / news123 |

### 4. Premier lancement des scrapers
```bash
# Dans le conteneur Airflow, déclencher le DAG manuellement
docker-compose exec airflow-scheduler airflow dags trigger news_batch_pipeline

# Ou directement via Python (hors Docker)
pip install -r requirements.txt
python scrapers/news_scraper.py    # Scrape et sauvegarde en Bronze
python etl/medallion_pipeline.py   # Bronze → Silver → Gold
python quality/quality_checks.py   # Contrôle qualité
```

### 5. Grafana Dashboard
1. Ouvrir http://localhost:3000
2. Le dashboard "News Platform — Tendances Médias" est auto-importé
3. Si absent : Dashboards → Import → coller le contenu de `grafana/dashboards/news_trends.json`

## Structure des fichiers
```
news-platform/
├── docker-compose.yml          # Tous les services
├── requirements.txt            # Dépendances Python
├── scrapers/
│   └── news_scraper.py        # Scraping BBC + Al Jazeera
├── etl/
│   └── medallion_pipeline.py  # Bronze → Silver → Gold
├── dags/
│   └── news_pipeline_dag.py   # DAGs Airflow (batch + streaming)
├── quality/
│   └── quality_checks.py      # Contrôles qualité
├── sql/
│   └── init.sql               # Schéma PostgreSQL
└── grafana/
    ├── dashboards/
    │   └── news_trends.json   # Dashboard JSON
    └── provisioning/
        ├── datasources/postgres.yml
        └── dashboards/dashboards.yml
```

## DAGs Airflow
| DAG | Fréquence | Description |
|-----|-----------|-------------|
| `news_batch_pipeline` | Toutes les heures | Scraping + pipeline Médaillon complet |
| `news_streaming_consumer` | Toutes les 5 min | Consommation des événements Kafka |

## Contrôles qualité
- Articles sans titre → rejetés
- Articles sans date → flagués
- Contenu < 30 mots → rejetés
- Doublons URL → dédupliqués
- Score global journalisé dans `data_quality_log`

## Arrêter la plateforme
```bash
docker-compose down           # Arrête les conteneurs
docker-compose down -v        # Arrête + supprime les données
```
