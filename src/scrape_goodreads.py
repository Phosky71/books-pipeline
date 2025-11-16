import json
import os
import re
import time
from datetime import datetime

import requests
from bs4 import BeautifulSoup

# Configuración principal
HEADERS = {
    "User-Agent": "https://github.com/Phosky71/books-pipeline)"
}
SEARCH_URL = "https://www.goodreads.com/search?q=bitcoin"
PAUSE = 0.7
N_TARGET = 15


def clean_isbn(raw):
    found = re.findall(r"(\d{10,13})", raw or "")
    isbn10 = next((i for i in found if len(i) == 10), None)
    isbn13 = next((i for i in found if len(i) == 13), None)
    return isbn10, isbn13


def scrape_goodreads():
    print("Scraping Goodreads…")
    # Asegura carpetas de destino
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    landing_dir = os.path.join(base_dir, "landing")
    os.makedirs(landing_dir, exist_ok=True)
    out_json = os.path.join(landing_dir, "goodreads_books.json")

    r = requests.get(SEARCH_URL, headers=HEADERS)
    soup = BeautifulSoup(r.text, "lxml")
    results = soup.select("tr[itemtype='http://schema.org/Book']")
    books = []
    for result in results:
        title = result.select_one("a.bookTitle span")
        author = result.select_one("a.authorName span")
        rating_elem = result.select_one("span.minirating")
        book_url_elem = result.select_one("a.bookTitle")

        title = title.get_text(strip=True) if title else None
        author = author.get_text(strip=True) if author else None
        book_url = (
            "https://www.goodreads.com" + book_url_elem["href"]
            if book_url_elem else None
        )
        isbn10, isbn13 = clean_isbn(book_url or "")
        rating, ratings_count = None, None
        if rating_elem:
            rating_text = rating_elem.get_text()
            rating_match = re.search(r"([0-9.]+) avg rating", rating_text)
            count_match = re.search(r"— ([\d,]+) ratings", rating_text)
            rating = float(rating_match.group(1)) if rating_match else None
            ratings_count = int(count_match.group(1).replace(",", "")) if count_match else None

        books.append({
            "title": title,
            "author": author,
            "rating": rating,
            "ratings_count": ratings_count,
            "book_url": book_url,
            "isbn10": isbn10,
            "isbn13": isbn13
        })
        time.sleep(PAUSE)
        if len(books) >= N_TARGET:
            break

    meta = {
        "scrape_url": SEARCH_URL,
        "user-agent": HEADERS["User-Agent"],
        "fecha_scraping": datetime.now().isoformat(),
        "n_registros": len(books),
        "campos": ["title", "author", "rating", "ratings_count", "book_url", "isbn10", "isbn13"],
        "selectores": {
            "title": "a.bookTitle span",
            "author": "a.authorName span",
            "rating": "span.minirating",
            "book_url": "a.bookTitle"
        }
    }
    out = {"meta": meta, "data": books}
    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2, ensure_ascii=False)
    print(f"{len(books)} libros guardados en {out_json}.")


if __name__ == "__main__":
    scrape_goodreads()
