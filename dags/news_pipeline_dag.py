"""
dags/news_pipeline_dag.py
DAG Airflow principal :
  - Scraping BBC + Al Jazeera toutes les heures (batch)
  - Pipeline Médaillon Bronze → Silver → Gold
  - Contrôle qualité des données
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# ─── Paramètres par défaut ────────────────────────────────────────────────────
default_args = {
    "owner":            "data-team",
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry":   False,
}

# ══════════════════════════════════════════════════════════════════════════════
# DAG 1 : Batch Pipeline (toutes les heures)
# ══════════════════════════════════════════════════════════════════════════════
with DAG(
    dag_id="news_batch_pipeline",
    description="Scraping + pipeline Médaillon toutes les heures",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 * * * *",   # Toutes les heures
    catchup=False,
    tags=["batch", "scraping", "medallion"],
) as dag_batch:

    def task_scrape_bbc(**ctx):
        import sys
        sys.path.insert(0, "/opt/airflow/scrapers")
        from news_scraper import scrape_bbc, get_minio_client, save_to_bronze
        client = get_minio_client()
        articles = scrape_bbc()
        count = sum(1 for a in articles if save_to_bronze(client, a))
        print(f"BBC : {count} articles sauvegardés en Bronze")
        ctx["ti"].xcom_push(key="bbc_count", value=count)

    def task_scrape_aljazeera(**ctx):
        import sys
        sys.path.insert(0, "/opt/airflow/scrapers")
        from news_scraper import scrape_aljazeera, get_minio_client, save_to_bronze
        client = get_minio_client()
        articles = scrape_aljazeera()
        count = sum(1 for a in articles if save_to_bronze(client, a))
        print(f"Al Jazeera : {count} articles sauvegardés en Bronze")
        ctx["ti"].xcom_push(key="aj_count", value=count)

    def task_silver_transform(**ctx):
        import sys
        sys.path.insert(0, "/opt/airflow/etl")
        from medallion_pipeline import get_minio, process_bronze_to_silver
        minio = get_minio()
        articles = process_bronze_to_silver(minio)
        print(f"Silver : {len(articles)} articles transformés")
        ctx["ti"].xcom_push(key="silver_count", value=len(articles))
        return articles

    def task_gold_load(**ctx):
        import sys
        sys.path.insert(0, "/opt/airflow/etl")
        from medallion_pipeline import get_minio, get_db, process_bronze_to_silver
        from medallion_pipeline import load_articles_to_warehouse, compute_daily_trends, compute_source_stats

        minio = get_minio()
        conn  = get_db()
        try:
            articles = process_bronze_to_silver(minio)
            load_articles_to_warehouse(conn, articles)
            compute_daily_trends(conn, articles)
            compute_source_stats(conn, articles)
            print(f"Gold : {len(articles)} articles chargés dans PostgreSQL")
        finally:
            conn.close()

    def task_quality_check(**ctx):
        import sys
        sys.path.insert(0, "/opt/airflow/quality")
        from quality_checks import run_quality_checks
        report = run_quality_checks()
        print(f"Qualité : {report['quality_rate']:.1%} ({report['passed']}/{report['total_checked']})")
        if report["quality_rate"] < 0.5:
            raise ValueError(f"Qualité trop basse : {report['quality_rate']:.1%}")

    # ─── Tâches ───────────────────────────────────────────────────────────────
    t_bbc = PythonOperator(
        task_id="scrape_bbc",
        python_callable=task_scrape_bbc,
    )

    t_aj = PythonOperator(
        task_id="scrape_aljazeera",
        python_callable=task_scrape_aljazeera,
    )

    t_silver = PythonOperator(
        task_id="bronze_to_silver",
        python_callable=task_silver_transform,
    )

    t_gold = PythonOperator(
        task_id="silver_to_gold",
        python_callable=task_gold_load,
    )

    t_quality = PythonOperator(
        task_id="quality_check",
        python_callable=task_quality_check,
    )

    # ─── Dépendances ──────────────────────────────────────────────────────────
    # Scraping BBC et AJ en parallèle → Silver → Gold → Qualité
    [t_bbc, t_aj] >> t_silver >> t_gold >> t_quality


# ══════════════════════════════════════════════════════════════════════════════
# DAG 2 : Streaming consumer Kafka (toutes les 5 min)
# ══════════════════════════════════════════════════════════════════════════════
with DAG(
    dag_id="news_streaming_consumer",
    description="Consomme les événements Kafka et les stocke en Bronze",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="*/5 * * * *",   # Toutes les 5 minutes
    catchup=False,
    tags=["streaming", "kafka"],
) as dag_stream:

    def task_consume_kafka(**ctx):
        """Consomme les messages Kafka du topic 'news-articles'."""
        import json
        import sys
        import os
        sys.path.insert(0, "/opt/airflow/scrapers")
        from news_scraper import get_minio_client, save_to_bronze, Article

        try:
            from kafka import KafkaConsumer
        except ImportError:
            print("kafka-python non installé, simulation mode")
            return

        consumer = KafkaConsumer(
            "news-articles",
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
            auto_offset_reset="earliest",
            consumer_timeout_ms=30000,  # 30s timeout
            value_deserializer=lambda m: json.loads(m.decode("utf-8"))
        )

        client = get_minio_client()
        count = 0
        for msg in consumer:
            art_data = msg.value
            art = Article(**art_data)
            if save_to_bronze(client, art):
                count += 1
            if count >= 100:  # Limite par run
                break

        consumer.close()
        print(f"Kafka : {count} articles consommés et sauvegardés en Bronze")

    t_kafka = PythonOperator(
        task_id="consume_kafka_events",
        python_callable=task_consume_kafka,
    )