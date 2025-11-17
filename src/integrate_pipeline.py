import hashlib
import json
import os

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from src.utils_isbn import normalize_isbn10

# --- Rutas absolutas base ---
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
JSON_PATH = os.path.join(BASE_DIR, "landing", "goodreads_books.json")
CSV_PATH = os.path.join(BASE_DIR, "landing", "googlebooks_books.csv")
PARQUET_DIM_PATH = os.path.join(BASE_DIR, "standard", "dim_book.parquet")
QUALITY_METRICS_PATH = os.path.join(BASE_DIR, "docs", "quality_metrics.json")


def normalize_isbn(isbn):
    if pd.isna(isbn): return None
    x = str(isbn).strip().replace("-", "")
    return x if len(x) == 13 and x.isdigit() else None


def normalize_string(text):
    if pd.isna(text) or text is None: return ""
    import re
    s = str(text).lower().strip()
    s = re.sub(r'[^a-z0-9 ]', '', s)
    return re.sub(r'\s+', ' ', s).strip()


def get_first_word(text):
    words = normalize_string(text).split()
    return words[0] if words else ""


def gen_hash(*args):
    s = '|'.join([str(a or '') for a in args])
    return hashlib.sha1(s.encode()).hexdigest()[:16]


def unify_and_normalize(df):
    # Título único
    df['titulo'] = (
        df['title'] if 'title' in df else
        df['titulo'] if 'titulo' in df else
        df['titulo_normalizado'] if 'titulo_normalizado' in df else ''
    )
    # Autores (en str)
    if 'authors' in df:
        df['autores'] = df['authors'].apply(lambda x: ', '.join(x) if isinstance(x, list) else x)
    elif 'author' in df:
        df['autores'] = df['author']
    else:
        df['autores'] = df['autores'] if 'autores' in df else ''
    # ISBN
    df['isbn13'] = df['isbn13'].apply(normalize_isbn) if 'isbn13' in df else None
    df['isbn10'] = df['isbn10'].apply(normalize_isbn10) if 'isbn10' in df else None
    if 'pub_date' in df:
        df['fecha_publicacion'] = df['pub_date']
    elif 'fecha_publicacion' not in df:
        df['fecha_publicacion'] = None
    if 'categories' in df:
        df['categoria'] = df['categories']
    elif "categoria" not in df:
        df["categoria"] = None
    if 'publisher' in df:
        df['editorial'] = df['publisher']
    elif 'editorial' not in df:
        df['editorial'] = None
    if 'language' in df:
        df['idioma'] = df['language']
    elif 'idioma' not in df:
        df['idioma'] = None
    return df


with open(JSON_PATH, encoding="utf-8") as f:
    gd_df = pd.DataFrame(json.load(f)["data"])
gb_df = pd.read_csv(CSV_PATH, encoding="utf-8")
gd_df["fuente"] = "goodreads"
gb_df["fuente"] = "googlebooks"
gd_df = unify_and_normalize(gd_df)
gb_df = unify_and_normalize(gb_df)
df = pd.concat([gd_df, gb_df], ignore_index=True, sort=False)

# --- Añadir clave de deduplicación solo por la primera palabra ---
df["titulo_first"] = df["titulo"].apply(get_first_word)
df["autores_first"] = df["autores"].apply(get_first_word)


def get_dup_key(row):
    return gen_hash(row.get("titulo_first", ""), row.get("autores_first", ""))


df["dup_key"] = df.apply(get_dup_key, axis=1)


# --- Deduplicación: elegimos el registro con menos nulos y preferimos googlebooks en empate ---
def pick_best_group(group):
    numeric_cols = ['rating', 'ratings_count', 'price_amount']
    group = group.copy()
    group["nulos"] = group.isnull().sum(axis=1)
    group["fuente_prio"] = group["fuente"].map({"googlebooks": 0, "goodreads": 1})
    group = group.sort_values(by=["nulos", "fuente_prio"], ascending=[True, True])
    win = group.iloc[0].copy()
    for col in group.columns:
        if win.get(col) in [None, '', np.nan] or pd.isna(win.get(col)):
            candidates = group[col].dropna()
            if len(candidates) > 0:
                if col in numeric_cols:
                    win[col] = candidates.iloc[0]
                else:
                    win[col] = candidates.astype(str).iloc[candidates.astype(str).str.len().argmax()]
    return win


deduped = df.groupby("dup_key", as_index=False).apply(lambda g: pick_best_group(g)).reset_index(drop=True)

# --- Selecciona y renombra sólo los campos finales estándar (elimina cualquier repetido) ---
campos_finales = [
    "titulo", "autores", "isbn13", "isbn10", "fecha_publicacion",
    "idioma", "editorial", "categoria", "rating", "ratings_count", "price_amount", "fuente"
]
deduped = deduped[campos_finales]

# --- Exportar ---
table = pa.Table.from_pandas(deduped.astype(str))
pq.write_table(table, PARQUET_DIM_PATH)
deduped.to_csv(PARQUET_DIM_PATH.replace(".parquet", ".csv"), index=False, encoding="utf-8")

print(f"\n✅ Libros únicos: {len(deduped)}")
print("Columnas finales:", list(deduped.columns))
