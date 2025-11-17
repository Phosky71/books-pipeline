import json
import os
import re
import time
from datetime import datetime

import requests
from bs4 import BeautifulSoup

# --- Configuración ---
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}

SEARCH_URL = "https://www.goodreads.com/search?q=non-fiction"
N_TARGET = 20  # Número de libros objetivo
MAX_PAGES = 5  # Máximo de páginas a scrapear
PAUSE = 1.5  # Pausa entre requests


def extract_isbn_from_book(book_url):
    time.sleep(PAUSE)
    try:
        r = requests.get(book_url, headers=HEADERS, timeout=10)
        if not r.ok:
            return None, None

        text = r.text

        # Busca el ISBN13 y ISBN10 en el JSON embebido
        m13 = re.search(r'"isbn13"\s*:\s*"(\d{13})"', text)
        m10 = re.search(r'"isbn"\s*:\s*"(\d{10})"', text)

        isbn13 = m13.group(1) if m13 else None
        isbn10 = m10.group(1) if m10 else None

        return isbn13, isbn10

    except Exception as e:
        print(f"  ⚠️  Error: {e}")
        return None, None


def scrape_goodreads():
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    landing_dir = os.path.join(base_dir, "landing")
    os.makedirs(landing_dir, exist_ok=True)
    out_json = os.path.join(landing_dir, "goodreads_books.json")

    books = []
    page = 1

    print(f"\n{'=' * 80}")
    print(f"🔍 Iniciando scraping de Goodreads")
    print(f"   Objetivo: {N_TARGET} libros")
    print(f"   Máximo páginas: {MAX_PAGES}")
    print(f"{'=' * 80}\n")

    while len(books) < N_TARGET and page <= MAX_PAGES:
        page_url = f"{SEARCH_URL}&page={page}" if page > 1 else SEARCH_URL
        print(f"📄 Página {page}...", end=" ")

        try:
            r = requests.get(page_url, headers=HEADERS, timeout=10)
            if not r.ok:
                print(f"❌ Error HTTP {r.status_code}")
                break

            soup = BeautifulSoup(r.text, "lxml")
            results = soup.select("tr[itemtype='http://schema.org/Book']")

            if not results:
                print(f"❌ Sin resultados")
                break

            print(f"✓ {len(results)} libros")

            for result in results:
                if len(books) >= N_TARGET:
                    break

                title_tag = result.select_one("a.bookTitle span")
                author_tag = result.select_one("a.authorName span")
                rating_tag = result.select_one("span.minirating")
                book_link_tag = result.select_one("a.bookTitle")

                if not title_tag or not book_link_tag:
                    continue

                title = title_tag.get_text(strip=True)
                author = author_tag.get_text(strip=True) if author_tag else ""
                book_url = "https://www.goodreads.com" + book_link_tag["href"]

                rating = None
                ratings_count = None
                if rating_tag:
                    rating_text = rating_tag.get_text(strip=True)
                    rating_match = re.search(r'([\d.]+)', rating_text)
                    count_match = re.search(r'([\d,]+)\s+rating', rating_text)
                    if rating_match:
                        rating = float(rating_match.group(1))
                    if count_match:
                        ratings_count = int(count_match.group(1).replace(',', ''))

                # Entra en el libro para extraer ISBN
                print(f"  [{len(books) + 1:2d}] {title[:40]:40}... ", end="")
                isbn13, isbn10 = extract_isbn_from_book(book_url)

                if isbn13:
                    print(f"✓ {isbn13}")
                elif isbn10:
                    print(f"✓ {isbn10}")
                else:
                    print(f"✗")

                books.append({
                    "title": title,
                    "author": author,
                    "rating": rating,
                    "ratings_count": ratings_count,
                    "book_url": book_url,
                    "isbn10": isbn10,
                    "isbn13": isbn13
                })

            page += 1
            if len(books) < N_TARGET:
                time.sleep(PAUSE)

        except Exception as e:
            print(f"❌ Error: {e}")
            break

    # Guardar resultados
    meta = {
        "scrape_url": SEARCH_URL,
        "user-agent": HEADERS["User-Agent"],
        "fecha_scraping": datetime.now().isoformat(),
        "n_registros": len(books),
        "n_paginas_scrapeadas": page - 1,
        "campos": ["title", "author", "rating", "ratings_count", "book_url", "isbn10", "isbn13"]
    }

    output = {"meta": meta, "data": books}

    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    print(f"\n{'=' * 80}")
    print(f"✅ Scraping completado!")
    print(f"   📚 Total libros: {len(books)}")
    print(f"   📄 Páginas procesadas: {page - 1}")
    print(f"   📋 ISBNs encontrados: {sum(1 for b in books if b.get('isbn13') or b.get('isbn10'))}")
    print(f"   💾 Guardado en: {out_json}")
    print(f"{'=' * 80}\n")


if __name__ == "__main__":
    scrape_goodreads()
