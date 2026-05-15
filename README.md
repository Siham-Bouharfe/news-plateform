News Platform
    Plateforme de collecte et d'analyse d'articles de presse en temps réel. Elle scrape automatiquement BBC News et Al Jazeera, stocke les données dans un Data Lake MinIO, les traite via un pipeline ETL, et affiche les tendances dans Grafana.

Stack
    Scraping : Python + BeautifulSoup
    Streaming : Apache Kafka
    Data Lake : MinIO (architecture Bronze / Silver / Gold)
    Orchestration : Apache Airflow
    Data Warehouse : PostgreSQL
    Visualisation : Grafana
    Déploiement : Docker Compose

Lancer le projet
    git clone <repo>
    cd news-platform
    docker-compose up -d

    Attendre ~3 minutes le temps que tout démarre, puis :
    Interface                   URL                         Identifiants
    Airflow                http://localhost:8080           admin / admin
    MinIO                  http://localhost:9001           minioadmin / minioadmin
    Grafana                http://localhost:3000           admin / admin

Comment ça marche
    Airflow déclenche un scraping toutes les heures. Les articles passent par trois couches :

        Bronze — JSON brut stocké dans MinIO tel quel
        Silver — nettoyage HTML, détection de langue, extraction de mots-clés
        Gold — agrégations chargées dans PostgreSQL (tendances, volumes par source)

    Grafana lit directement PostgreSQL et rafraîchit les dashboards toutes les 5 minutes.

Structure
    news-platform/
    ├── scrapers/          # BBC + Al Jazeera
    ├── etl/               # Pipeline Bronze → Silver → Gold
    ├── dags/              # DAGs Airflow
    ├── quality/           # Contrôles qualité
    ├── sql/               # Schéma PostgreSQL
    ├── grafana/           # Dashboard + provisioning
    └── docker-compose.yml

Vérifier que les données arrivent
    docker-compose exec postgres psql -U news -d newsdb \
  -c "SELECT source, COUNT(*) FROM articles GROUP BY source;"

Arrêt
    docker-compose down 