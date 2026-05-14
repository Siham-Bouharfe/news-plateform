"""
etl/medallion_pipeline.py
Pipeline Médaillon complet :
  Bronze  → articles bruts depuis MinIO
  Silver  → nettoyage, normalisation, détection langue
  Gold    → agrégations dans PostgreSQL (Data Warehouse)
"""

import os
import re
import json
import logging
from datetime import datetime, date, timezone
from collections import Counter
from io import BytesIO
from typing import Optional, List, Dict, Tuple

import psycopg2
from minio import Minio
from minio.error import S3Error

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# ─── Config ───────────────────────────────────────────────────────────────────
MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT", "localhost:9000").replace("http://", "")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")

DB_CONFIG = {
    "host":     os.getenv("POSTGRES_HOST", "localhost"),
    "dbname":   os.getenv("POSTGRES_DB", "newsdb"),
    "user":     os.getenv("POSTGRES_USER", "news"),
    "password": os.getenv("POSTGRES_PASSWORD", "news123"),
    "port":     5432
}

STOP_WORDS = {
    "the", "a", "an", "and", "or", "but", "in", "on", "at", "to", "for",
    "of", "with", "by", "from", "is", "it", "its", "as", "this", "that",
    "was", "are", "be", "been", "have", "has", "had", "will", "would",
    "could", "should", "may", "might", "said", "says", "also", "after",
    "before", "during", "over", "under", "more", "most", "his", "her",
    "their", "our", "your", "who", "which", "when", "where", "how", "what"
}


# ─── Connexions ───────────────────────────────────────────────────────────────
def get_minio() -> Minio:
    return Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS_KEY,
                 secret_key=MINIO_SECRET_KEY, secure=False)

def get_db():
    return psycopg2.connect(**DB_CONFIG)


# ══════════════════════════════════════════════════════════════════════════════
# BRONZE → SILVER : Nettoyage et normalisation
# ══════════════════════════════════════════════════════════════════════════════

def clean_html(text: str) -> str:
    """Supprime les balises HTML résiduelles."""
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"&[a-zA-Z]+;", " ", text)
    return text.strip()

def normalize_text(text: str) -> str:
    """Normalise les espaces et caractères spéciaux."""
    text = re.sub(r"\s+", " ", text)
    text = re.sub(r"[^\w\s.,!?;:'\"()\-–—]", "", text)
    return text.strip()

def detect_language(text: str) -> str:
    """Détection de langue simple par mots-clés fréquents."""
    sample = text.lower()[:500]
    fr_words = {"le", "la", "les", "un", "une", "des", "et", "est", "au", "du"}
    ar_words = {"في", "من", "إلى", "على", "أن", "هذا", "هي"}

    fr_score = sum(1 for w in fr_words if f" {w} " in sample)
    ar_score = sum(1 for w in ar_words if w in sample)

    if ar_score > 2:
        return "ar"
    if fr_score > 3:
        return "fr"
    return "en"

def extract_keywords(text: str, top_n: int = 20) -> List[str]:
    """Extrait les mots-clés les plus fréquents (hors stop words)."""
    words = re.findall(r"\b[a-zA-Z]{4,}\b", text.lower())
    words = [w for w in words if w not in STOP_WORDS]
    counter = Counter(words)
    return [word for word, _ in counter.most_common(top_n)]

def silver_transform(raw: dict) -> Optional[dict]:
    """Transforme un article brut (Bronze) en article nettoyé (Silver)."""
    # Validation minimale
    if not raw.get("title") or len(raw.get("title", "")) < 5:
        log.warning(f"Article sans titre ignoré : {raw.get('url')}")
        return None
    if not raw.get("content") or len(raw.get("content", "")) < 100:
        log.warning(f"Contenu trop court ignoré : {raw.get('url')}")
        return None

    content_clean = normalize_text(clean_html(raw.get("content", "")))
    title_clean   = normalize_text(clean_html(raw.get("title", "")))

    # Parsing date
    pub_raw = raw.get("published_at", "")
    published_at = None
    if pub_raw:
        for fmt in ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%d"):
            try:
                published_at = datetime.strptime(pub_raw[:19], fmt[:len(pub_raw[:19])])
                break
            except Exception:
                continue

    return {
        "id":           raw.get("id"),
        "title":        title_clean,
        "author":       raw.get("author"),
        "published_at": published_at.isoformat() if published_at else None,
        "category":     (raw.get("category") or "general").lower().strip(),
        "content":      content_clean,
        "source":       raw.get("source"),
        "url":          raw.get("url"),
        "language":     detect_language(content_clean),
        "word_count":   len(content_clean.split()),
        "keywords":     extract_keywords(content_clean),
        "scraped_at":   raw.get("scraped_at"),
        "layer":        "silver"
    }


def process_bronze_to_silver(minio_client: Minio) -> List[dict]:
    """Lit tous les objets Bronze et produit les articles Silver."""
    silver_articles = []

    try:
        objects = list(minio_client.list_objects("bronze", recursive=True))
        log.info(f"[SILVER] {len(objects)} objets dans Bronze")

        for obj in objects:
            try:
                response = minio_client.get_object("bronze", obj.object_name)
                raw = json.loads(response.read().decode("utf-8"))
                silver = silver_transform(raw)
                if silver:
                    # Sauvegarder dans MinIO silver
                    data = json.dumps(silver, ensure_ascii=False).encode("utf-8")
                    key  = obj.object_name.replace("bronze/", "", 1) if False else obj.object_name
                    minio_client.put_object(
                        "silver", obj.object_name,
                        data=BytesIO(data), length=len(data),
                        content_type="application/json"
                    )
                    silver_articles.append(silver)
            except Exception as e:
                log.error(f"[SILVER] Erreur traitement {obj.object_name}: {e}")

    except S3Error as e:
        log.error(f"[SILVER] Erreur MinIO : {e}")

    log.info(f"[SILVER] {len(silver_articles)} articles transformés")
    return silver_articles


# ══════════════════════════════════════════════════════════════════════════════
# SILVER → GOLD : Agrégations dans PostgreSQL
# ══════════════════════════════════════════════════════════════════════════════

def load_articles_to_warehouse(conn, articles: List[dict]) -> int:
    """Insère les articles Silver dans PostgreSQL (table articles)."""
    inserted = 0
    with conn.cursor() as cur:
        for art in articles:
            try:
                cur.execute("""
                    INSERT INTO articles
                        (title, author, published_at, category, content, source,
                         url, language, word_count, layer)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (url) DO UPDATE SET
                        word_count = EXCLUDED.word_count,
                        layer = 'gold'
                """, (
                    art["title"], art.get("author"),
                    art.get("published_at"), art.get("category"),
                    art["content"], art["source"], art["url"],
                    art.get("language", "en"), art.get("word_count", 0),
                    "gold"
                ))
                inserted += 1
            except Exception as e:
                log.error(f"[GOLD] Erreur insert article : {e}")
    conn.commit()
    log.info(f"[GOLD] {inserted} articles insérés dans articles")
    return inserted


def compute_daily_trends(conn, articles: List[dict]):
    """Calcule les tendances quotidiennes (mots-clés) et les insère."""
    today = date.today()
    # Agréger keywords par source
    keyword_counts: Dict = {}
    for art in articles:
        source = art.get("source", "unknown")
        category = art.get("category", "general")
        for kw in art.get("keywords", [])[:10]:
            key = (today, kw.lower(), source, category)
            keyword_counts[key] = keyword_counts.get(key, 0) + 1

    with conn.cursor() as cur:
        for (d, kw, source, cat), count in keyword_counts.items():
            try:
                cur.execute("""
                    INSERT INTO daily_trends (trend_date, keyword, occurrence, source, category)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (trend_date, keyword, source)
                    DO UPDATE SET occurrence = daily_trends.occurrence + EXCLUDED.occurrence
                """, (d, kw, count, source, cat))
            except Exception as e:
                log.error(f"[GOLD] Erreur trend : {e}")
    conn.commit()
    log.info("[GOLD] Tendances quotidiennes mises à jour")


def compute_source_stats(conn, articles: List[dict]):
    """Calcule les statistiques par source."""
    today = date.today()
    from collections import defaultdict
    stats: dict[str, dict] = defaultdict(lambda: {"count": 0, "words": [], "cats": set()})

    for art in articles:
        src = art.get("source", "unknown")
        stats[src]["count"] += 1
        stats[src]["words"].append(art.get("word_count", 0))
        if art.get("category"):
            stats[src]["cats"].add(art["category"])

    with conn.cursor() as cur:
        for src, s in stats.items():
            avg_words = sum(s["words"]) / len(s["words"]) if s["words"] else 0
            try:
                cur.execute("""
                    INSERT INTO articles_by_source
                        (report_date, source, article_count, avg_word_count, categories)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (report_date, source)
                    DO UPDATE SET
                        article_count   = EXCLUDED.article_count,
                        avg_word_count  = EXCLUDED.avg_word_count,
                        categories      = EXCLUDED.categories
                """, (today, src, s["count"], avg_words, list(s["cats"])))
            except Exception as e:
                log.error(f"[GOLD] Erreur source stats : {e}")
    conn.commit()
    log.info("[GOLD] Stats par source mises à jour")


# ══════════════════════════════════════════════════════════════════════════════
# ORCHESTRATION DU PIPELINE COMPLET
# ══════════════════════════════════════════════════════════════════════════════

def run_full_pipeline():
    """Exécute le pipeline complet Bronze → Silver → Gold."""
    log.info("=== Démarrage pipeline Médaillon ===")

    minio = get_minio()
    conn  = get_db()

    try:
        # Bronze → Silver
        log.info("--- Étape 1 : Bronze → Silver ---")
        silver_articles = process_bronze_to_silver(minio)

        if not silver_articles:
            log.warning("Aucun article Silver produit. Pipeline arrêté.")
            return

        # Silver → Gold (PostgreSQL)
        log.info("--- Étape 2 : Silver → Gold ---")
        load_articles_to_warehouse(conn, silver_articles)
        compute_daily_trends(conn, silver_articles)
        compute_source_stats(conn, silver_articles)

        log.info(f"=== Pipeline terminé : {len(silver_articles)} articles traités ===")

    finally:
        conn.close()


if __name__ == "__main__":
    run_full_pipeline()