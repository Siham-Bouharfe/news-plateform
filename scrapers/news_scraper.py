"""
scrapers/news_scraper.py
Collecte des articles depuis BBC News et Al Jazeera.
Stocke les articles bruts dans MinIO (couche Bronze).
"""

import os
import json
import uuid
import logging
import hashlib
from datetime import datetime, timezone
from dataclasses import dataclass, asdict
from typing import Optional, List

import requests
from bs4 import BeautifulSoup
from minio import Minio
from minio.error import S3Error

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# ─── Configuration ────────────────────────────────────────────────────────────
MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT", "localhost:9000").replace("http://", "")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
BRONZE_BUCKET    = "bronze"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}

# ─── Modèle Article ───────────────────────────────────────────────────────────
@dataclass
class Article:
    id:           str
    title:        str
    author:       Optional[str]
    published_at: Optional[str]
    category:     Optional[str]
    content:      str
    source:       str
    url:          str
    scraped_at:   str
    language:     str = "en"

    @staticmethod
    def make_id(url: str) -> str:
        return hashlib.md5(url.encode()).hexdigest()


# ─── Client MinIO ─────────────────────────────────────────────────────────────
def get_minio_client() -> Minio:
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )


def save_to_bronze(client: Minio, article: Article) -> bool:
    """Sauvegarde un article JSON dans MinIO bucket bronze."""
    now = datetime.now(timezone.utc)
    key = f"{article.source}/{now.strftime('%Y/%m/%d')}/{article.id}.json"

    data = json.dumps(asdict(article), ensure_ascii=False, indent=2).encode("utf-8")
    try:
        from io import BytesIO
        client.put_object(
            BRONZE_BUCKET, key,
            data=BytesIO(data),
            length=len(data),
            content_type="application/json"
        )
        log.info(f"[BRONZE] Sauvegardé : {key}")
        return True
    except S3Error as e:
        log.error(f"[BRONZE] Erreur MinIO : {e}")
        return False


# ─── Scraper BBC News ─────────────────────────────────────────────────────────
def scrape_bbc() -> List[Article]:
    """Scrape la page principale de BBC News et extrait les articles."""
    articles = []
    base_url = "https://www.bbc.com/news"

    try:
        resp = requests.get(base_url, headers=HEADERS, timeout=15)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        # Extraire les liens d'articles BBC
        links = set()
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "/news/" in href and len(href) > 20:
                if href.startswith("/"):
                    href = "https://www.bbc.com" + href
                if "bbc.com/news/" in href:
                    links.add(href)

        log.info(f"[BBC] {len(links)} liens trouvés")

        for url in list(links)[:15]:  # Limite à 15 articles par run
            art = scrape_bbc_article(url)
            if art:
                articles.append(art)

    except Exception as e:
        log.error(f"[BBC] Erreur page principale : {e}")

    return articles


def scrape_bbc_article(url: str) -> Optional[Article]:
    """Scrape un article BBC individuel."""
    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        # Titre
        title_tag = (
            soup.find("h1", {"data-component": "headline-block"}) or
            soup.find("h1") or
            soup.find("title")
        )
        title = title_tag.get_text(strip=True) if title_tag else None
        if not title or len(title) < 5:
            return None

        # Contenu
        content_blocks = soup.find_all("div", {"data-component": "text-block"})
        if not content_blocks:
            content_blocks = soup.find_all("p")
        content = " ".join(b.get_text(strip=True) for b in content_blocks)
        if len(content) < 50:
            return None

        # Auteur
        author_tag = soup.find("span", {"class": lambda c: c and "author" in c.lower()})
        author = author_tag.get_text(strip=True) if author_tag else "BBC News"

        # Date
        time_tag = soup.find("time")
        published = time_tag.get("datetime") if time_tag else None

        # Catégorie (depuis l'URL)
        parts = url.replace("https://www.bbc.com/news/", "").split("-")
        category = parts[0] if parts else "general"

        return Article(
            id=Article.make_id(url),
            title=title,
            author=author,
            published_at=published,
            category=category,
            content=content[:5000],  # tronqué à 5000 chars
            source="BBC News",
            url=url,
            scraped_at=datetime.now(timezone.utc).isoformat(),
            language="en"
        )

    except Exception as e:
        log.warning(f"[BBC] Erreur article {url} : {e}")
        return None


# ─── Scraper Al Jazeera ───────────────────────────────────────────────────────
def scrape_aljazeera() -> List[Article]:
    """Scrape Al Jazeera English news."""
    articles = []
    base_url = "https://www.aljazeera.com/news/"

    try:
        resp = requests.get(base_url, headers=HEADERS, timeout=15)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        links = set()
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "/news/" in href and len(href) > 20:
                if href.startswith("/"):
                    href = "https://www.aljazeera.com" + href
                if "aljazeera.com" in href:
                    links.add(href)

        log.info(f"[Al Jazeera] {len(links)} liens trouvés")

        for url in list(links)[:15]:
            art = scrape_aljazeera_article(url)
            if art:
                articles.append(art)

    except Exception as e:
        log.error(f"[Al Jazeera] Erreur page principale : {e}")

    return articles


def scrape_aljazeera_article(url: str) -> Optional[Article]:
    """Scrape un article Al Jazeera individuel."""
    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        # Titre
        title_tag = soup.find("h1", {"class": lambda c: c and "article" in str(c).lower()}) or soup.find("h1")
        title = title_tag.get_text(strip=True) if title_tag else None
        if not title or len(title) < 5:
            return None

        # Contenu
        article_body = soup.find("div", {"class": lambda c: c and "wysiwyg" in str(c).lower()})
        if article_body:
            content = " ".join(p.get_text(strip=True) for p in article_body.find_all("p"))
        else:
            content = " ".join(p.get_text(strip=True) for p in soup.find_all("p"))
        if len(content) < 50:
            return None

        # Auteur
        author_tag = soup.find("a", {"class": lambda c: c and "author" in str(c).lower()})
        author = author_tag.get_text(strip=True) if author_tag else "Al Jazeera"

        # Date
        time_tag = soup.find("time") or soup.find("meta", {"property": "article:published_time"})
        if time_tag:
            published = time_tag.get("datetime") or time_tag.get("content")
        else:
            published = None

        # Catégorie
        category_tag = soup.find("a", {"class": lambda c: c and "topic" in str(c).lower()})
        category = category_tag.get_text(strip=True) if category_tag else "world"

        return Article(
            id=Article.make_id(url),
            title=title,
            author=author,
            published_at=published,
            category=category,
            content=content[:5000],
            source="Al Jazeera",
            url=url,
            scraped_at=datetime.now(timezone.utc).isoformat(),
            language="en"
        )

    except Exception as e:
        log.warning(f"[Al Jazeera] Erreur article {url} : {e}")
        return None


# ─── Point d'entrée principal ─────────────────────────────────────────────────
def run_scrapers():
    """Lance tous les scrapers et sauvegarde dans Bronze."""
    client = get_minio_client()

    total = 0
    for scraper_fn, name in [(scrape_bbc, "BBC"), (scrape_aljazeera, "Al Jazeera")]:
        log.info(f"=== Scraping {name} ===")
        articles = scraper_fn()
        for art in articles:
            if save_to_bronze(client, art):
                total += 1

    log.info(f"=== Terminé : {total} articles sauvegardés dans Bronze ===")
    return total


if __name__ == "__main__":
    run_scrapers()