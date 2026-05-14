"""
quality/quality_checks.py
Contrôles qualité des données :
  - Articles sans titre
  - Dates manquantes
  - Contenu trop court
  - Doublons
  - Score global de qualité
"""

import os
import logging
import psycopg2
from datetime import datetime

log = logging.getLogger(__name__)

DB_CONFIG = {
    "host":     os.getenv("POSTGRES_HOST", "localhost"),
    "dbname":   os.getenv("POSTGRES_DB", "newsdb"),
    "user":     os.getenv("POSTGRES_USER", "news"),
    "password": os.getenv("POSTGRES_PASSWORD", "news123"),
    "port":     5432
}

MIN_CONTENT_WORDS = 30  # Seuil contenu trop court


def run_quality_checks(pipeline_run: str = None) -> dict:
    """Exécute tous les contrôles qualité et journalise les résultats."""
    if not pipeline_run:
        pipeline_run = f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    conn = psycopg2.connect(**DB_CONFIG)
    report = {}

    try:
        with conn.cursor() as cur:
            # Total articles dans la base
            cur.execute("SELECT COUNT(*) FROM articles")
            total = cur.fetchone()[0]

            # Articles sans titre
            cur.execute("SELECT COUNT(*) FROM articles WHERE title IS NULL OR title = ''")
            missing_title = cur.fetchone()[0]

            # Articles sans date de publication
            cur.execute("SELECT COUNT(*) FROM articles WHERE published_at IS NULL")
            missing_date = cur.fetchone()[0]

            # Articles avec contenu trop court (moins de 30 mots)
            cur.execute(f"SELECT COUNT(*) FROM articles WHERE word_count < {MIN_CONTENT_WORDS}")
            too_short = cur.fetchone()[0]

            # Doublons (même URL)
            cur.execute("""
                SELECT COUNT(*) FROM (
                    SELECT url, COUNT(*) as cnt
                    FROM articles
                    GROUP BY url
                    HAVING COUNT(*) > 1
                ) dup
            """)
            duplicates = cur.fetchone()[0]

            # Calcul score qualité (date manquante = avertissement, pas bloquant)
            issues = missing_title + too_short + duplicates
            passed = max(0, total - issues)
            quality_rate = (passed / total) if total > 0 else 0.0

            report = {
                "pipeline_run":  pipeline_run,
                "total_checked": total,
                "missing_title": missing_title,
                "missing_date":  missing_date,
                "too_short":     too_short,
                "duplicates":    duplicates,
                "passed":        passed,
                "quality_rate":  quality_rate
            }

            # Journaliser dans la base
            cur.execute("""
                INSERT INTO data_quality_log
                    (pipeline_run, total_checked, missing_title, missing_date,
                     too_short, duplicates, passed, quality_rate)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            """, (
                pipeline_run, total, missing_title, missing_date,
                too_short, duplicates, passed, quality_rate
            ))
            conn.commit()

            # Log rapport
            log.info(f"=== Rapport Qualité [{pipeline_run}] ===")
            log.info(f"  Total articles    : {total}")
            log.info(f"  Sans titre        : {missing_title}")
            log.info(f"  Sans date         : {missing_date}")
            log.info(f"  Contenu trop court: {too_short}")
            log.info(f"  Doublons URL      : {duplicates}")
            log.info(f"  Score qualité     : {quality_rate:.1%}")

    finally:
        conn.close()

    return report


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    report = run_quality_checks()
    print(f"\nScore qualité final : {report['quality_rate']:.1%}")