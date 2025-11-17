import json
import os
import time

import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("GOOGLE_BOOKS_API_KEY")
base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INPUT_JSON = os.path.join(base_dir, "landing", "goodreads_books.json")
OUTPUT_CSV = os.path.join(base_dir, "landing", "googlebooks_books.csv")
API_URL = "https://www.googleapis.com/books/v1/volumes"
PAUSE = 1.0

FIELDS = [
    "gb_id", "title", "subtitle", "authors", "publisher", "pub_date", "language",
    "categories", "isbn13", "isbn10", "price_amount", "price_currency"
]


def fetch_google_books(query):
    params = {
        "q": query,
        "key": API_KEY,
        "maxResults": 1
    }
    r = requests.get(API_URL, params=params)
    if r.status_code == 200:
        items = r.json().get("items", [])
        if items:
            return items[0]
    return None


def parse_gbook(item):
    volume = item.get("volumeInfo", {})
    sale = item.get("saleInfo", {})

    gbid = item.get("id")
    title = volume.get("title")
    subtitle = volume.get("subtitle")
    authors = ",".join(volume.get("authors", []))
    publisher = volume.get("publisher")
    pub_date = volume.get("publishedDate")
    language = volume.get("language")
    categories = ",".join(volume.get("categories", []))

    isbn13, isbn10 = None, None
    for iden in volume.get("industryIdentifiers", []):
        if iden.get("type") == "ISBN_13":
            isbn13 = iden.get("identifier")
        elif iden.get("type") == "ISBN_10":
            isbn10 = iden.get("identifier")

    price_amount, price_currency = None, None
    if sale.get("saleability") == "FOR_SALE" and "retailPrice" in sale:
        price_amount = sale["retailPrice"].get("amount")
        price_currency = sale["retailPrice"].get("currencyCode")

    return {
        "gb_id": gbid,
        "title": title,
        "subtitle": subtitle,
        "authors": authors,
        "publisher": publisher,
        "pub_date": pub_date,
        "language": language,
        "categories": categories,
        "isbn13": isbn13,
        "isbn10": isbn10,
        "price_amount": price_amount,
        "price_currency": price_currency
    }


def search_book(book):
    # 1. Buscar por ISBN13
    if book.get("isbn13"):
        query = f"isbn:{book['isbn13']}"
        item = fetch_google_books(query)
        if item:
            return item, query
    # 2. Si no, buscar por ISBN10
    if book.get("isbn10"):
        query = f"isbn:{book['isbn10']}"
        item = fetch_google_books(query)
        if item:
            return item, query
    # 3. Si no, buscar por título+autor
    title = book.get("title", "").replace(" ", "+")
    author = book.get("author", "").replace(" ", "+")
    query = f"intitle:{title}+inauthor:{author}"
    item = fetch_google_books(query)
    if item:
        return item, query
    # Nada encontrado
    return None, None


def main():
    with open(INPUT_JSON, encoding='utf-8') as f:
        gd_data = json.load(f)["data"]

    gbooks_records = []
    for i, book in enumerate(gd_data):
        item, query = search_book(book)
        if item:
            record = parse_gbook(item)
        else:
            record = {k: None for k in FIELDS}
        gbooks_records.append(record)
        print(f"[{i + 1}/{len(gd_data)}] {query if query else '-'} -> {'OK' if item else 'N/A'}")
        time.sleep(PAUSE)

    df = pd.DataFrame(gbooks_records, columns=FIELDS)
    df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8", sep=",")
    print(f"Guardado {len(df)} registros en {OUTPUT_CSV} (sep=',' encoding='utf-8').")


if __name__ == "__main__":
    main()
