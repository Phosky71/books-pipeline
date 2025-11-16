import hashlib
import json
import os
from datetime import datetime

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# --- Rutas absolutas base ---
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
JSON_PATH = os.path.join(BASE_DIR, "landing", "goodreads_books.json")
CSV_PATH = os.path.join(BASE_DIR, "landing", "googlebooks_books.csv")
PARQUET_DIM_PATH = os.path.join(BASE_DIR, "standard", "dim_book.parquet")
PARQUET_DETAIL_PATH = os.path.join(BASE_DIR, "standard", "book_source_detail.parquet")
QUALITY_METRICS_PATH = os.path.join(BASE_DIR, "docs", "quality_metrics.json")


# --- Funciones auxiliares ---
def normalize_isbn(isbn):
    if pd.isna(isbn): return None
    x = str(isbn).strip().replace("-", "")
    return x if len(x) == 13 else None


def normalize_date(date):
    try:
        if pd.isna(date): return None
        return pd.to_datetime(date, errors='coerce').strftime('%Y-%m-%d')
    except Exception:
        return None


def normalize_lang(lang):
    if isinstance(lang, str):
        return lang.lower().replace("_", "-")[:5]
    return None


def gen_hash(*args):
    s = '|'.join([str(a or '').strip().lower() for a in args])
    return hashlib.sha1(s.encode()).hexdigest()[:16]


def get_dup_key(row):
    isbn = normalize_isbn(row.get("isbn13", ""))
    if isbn:
        return isbn
    titulo = str(row.get("titulo_normalizado", "")).strip().lower()
    autores = row.get("autores", "")
    if isinstance(autores, list):
        autores = ",".join([str(a).strip().lower() for a in autores if a])
    else:
        autores = str(autores).strip().lower()
    editorial = str(row.get("editorial", "")).strip().lower()
    año = str(row.get("anio_publicacion", "")).strip()
    key = "|".join([titulo, autores, editorial, año])
    return gen_hash(key)



def is_empty(val):
    if pd.isna(val):
        return True
    if isinstance(val, np.ndarray):
        return val.size == 0
    if isinstance(val, (list, dict, str)):
        return len(val) == 0
    return val is None


# --- Leer datos ---
with open(JSON_PATH, encoding="utf-8") as f:
    gd_raw = json.load(f)
gd_books = pd.DataFrame(gd_raw["data"])
gb_books = pd.read_csv(CSV_PATH, encoding="utf-8")

# --- Limpia columnas all-nan antes de concatenar ---
gd_books = gd_books.dropna(axis=1, how='all')
gb_books = gb_books.dropna(axis=1, how='all')


# --- Normalización principal ---
def normalize_books(df, src):
    df = df.copy()
    if 'isbn13' in df:
        df["isbn13"] = df["isbn13"].apply(normalize_isbn)
    else:
        df["isbn13"] = None
    if 'fecha_publicacion' in df:
        df["fecha_publicacion"] = df["fecha_publicacion"].apply(normalize_date)
    elif 'pub_date' in df:
        df["fecha_publicacion"] = df["pub_date"].apply(normalize_date)
    else:
        df["fecha_publicacion"] = None
    if 'idioma' in df:
        df["idioma"] = df["idioma"].apply(normalize_lang)
    elif 'language' in df:
        df["idioma"] = df["language"].apply(normalize_lang)
    else:
        df["idioma"] = None
    df["fuente"] = src
    if "title" in df:
        df["titulo_normalizado"] = df["title"].astype(str).str.strip().str.lower()
    elif "titulo" in df:
        df["titulo_normalizado"] = df["titulo"].astype(str).str.strip().str.lower()
    else:
        df["titulo_normalizado"] = None
    if "authors" in df:
        df["autores"] = df["authors"]
    elif "author" in df:
        df["autores"] = df["author"]
    else:
        df["autores"] = None
    df["editorial"] = df.get("publisher", df.get("editorial", None))
    df["anio_publicacion"] = df.get("anio_publicacion", df.get("fecha_publicacion", None))
    df["ts_ultima_actualizacion"] = datetime.now().isoformat()
    return df


gd_books_n = normalize_books(gd_books, "goodreads")
gb_books_n = normalize_books(gb_books, "googlebooks")

all_books = pd.concat([gd_books_n, gb_books_n], ignore_index=True, sort=False)
all_books["dup_key"] = all_books.apply(get_dup_key, axis=1)


# --- Deduplicación con reglas de supervivencia ---
def dedup_supervivencia(df):
    deduped = []
    for key, group in df.groupby("dup_key"):
        # Ordena por menos nulos (más completo), luego GoogleBooks > Goodreads > más reciente
        group["fuente_prioridad"] = group["fuente"].map({"googlebooks": 0, "goodreads": 1})
        group = group.assign(num_nulos=group.isnull().sum(axis=1))
        group = group.sort_values(by=["num_nulos", "fuente_prioridad", "ts_ultima_actualizacion"],
                                  ascending=[True, True, False])
        win = group.iloc[0].copy()
        # Rellenar campos nulos con el mejor valor del grupo
        for col in df.columns:
            if is_empty(win[col]):
                best = group[col].dropna().astype(str)
                if not best.empty:
                    win[col] = best.iloc[best.str.len().argmax()]
        # Para listas/categorías, unir y deduplicar (autores, categorias)
        if "autores" in group:
            autores_set = set()
            for a in group["autores"].dropna():
                if isinstance(a, str):
                    autores_set.update(map(str.strip, a.split(",")))
                elif isinstance(a, list):
                    autores_set.update(a)
            win["autores"] = list({a for a in autores_set if a})
        if "categoria" in group:
            categorias = set()
            for c in group["categoria"].dropna():
                if isinstance(c, str):
                    categorias.update(map(str.strip, c.split(",")))
                elif isinstance(c, list):
                    categorias.update(c)
            win["categoria"] = list({c for c in categorias if c})
        win["fuente_ganadora"] = win["fuente"]
        deduped.append(win)
    return pd.DataFrame(deduped)


dimbook = dedup_supervivencia(all_books)

# --- Artefacto detalle por fuente ---
book_source_detail = all_books.copy()
book_source_detail["source_id"] = book_source_detail.index
book_source_detail["source_file"] = book_source_detail["fuente"].map({
    "goodreads": "goodreads_books.json",
    "googlebooks": "googlebooks_books.csv"
})
book_source_detail["row_number"] = book_source_detail.index + 1
book_source_detail["book_id_candidato"] = book_source_detail["dup_key"]
book_source_detail["timestamp_ingesta"] = datetime.now().isoformat()
book_source_detail["flags_val"] = book_source_detail.apply(lambda r: {"isbn_valido": bool(r["isbn13"])}, axis=1)
book_source_detail = book_source_detail[
    ["source_id", "fuente", "source_file", "row_number", "book_id_candidato"] +
    [c for c in all_books.columns if c not in ["fuente"]] +
    ["flags_val", "timestamp_ingesta"]]

# --- Quality Metrics ---
quality = {
    "total_libros_dimbook": len(dimbook),
    "%_isbn13_validos": float(np.mean(dimbook["isbn13"].notnull())) * 100,
    "%_titulos_validos": float(np.mean(dimbook["titulo_normalizado"].notnull())) * 100,
    "%_fechas_validas": float(np.mean(dimbook["fecha_publicacion"].notnull())) * 100,
    "%_idioma_bcp47": float(np.mean(dimbook["idioma"].notnull())) * 100,
    "%_moneda_iso4217": float(
        np.mean(dimbook["price_currency"].notnull())) * 100 if "price_currency" in dimbook else None,
    "duplicados_encontrados": int(len(all_books) - len(dimbook)),
    "nulos_por_campo": dimbook.isnull().sum().to_dict(),
    "filas_por_fuente": all_books["fuente"].value_counts().to_dict(),
    "generado_en": datetime.now().isoformat(),
}

# --- Guardar artefactos ---
os.makedirs(os.path.dirname(PARQUET_DIM_PATH), exist_ok=True)
os.makedirs(os.path.dirname(QUALITY_METRICS_PATH), exist_ok=True)
table1 = pa.Table.from_pandas(dimbook)
table2 = pa.Table.from_pandas(book_source_detail)
pq.write_table(table1, PARQUET_DIM_PATH)
pq.write_table(table2, PARQUET_DETAIL_PATH)
with open(QUALITY_METRICS_PATH, "w", encoding="utf-8") as f:
    json.dump(quality, f, indent=2, ensure_ascii=False)

print(
    f"Integración completa.\nDimensión libros: {PARQUET_DIM_PATH}\nDetalle fuentes: {PARQUET_DETAIL_PATH}\nMetrics: {QUALITY_METRICS_PATH}")
