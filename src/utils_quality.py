import numpy as np
import pandas as pd


def null_percentage(df):
    # Devuelve % de nulos por campo
    return df.isnull().mean().to_dict()


def uniqueness_check(df, field):
    # Comprueba unicidad de una clave (ej: isbn13 o id_canónico)
    total = len(df)
    unicos = df[field].nunique(dropna=True)
    return unicos == total


def value_ranges(df, field, min_value=None, max_value=None):
    # Chequea que un campo esté en rango permitido
    if min_value is not None and (df[field].dropna() < min_value).any():
        return False
    if max_value is not None and (df[field].dropna() > max_value).any():
        return False
    return True


def validate_date_iso(df, field):
    # Chequea formato ISO-8601 en fechas
    try:
        dates = pd.to_datetime(df[field], errors='coerce')
        return float(np.mean(dates.notnull())) * 100
    except Exception:
        return 0


def validate_language(df, field):
    # Verifica idioma BCP-47 simple (2-5 chars, letras/números, minúsculas)
    return float(np.mean(df[field].str.match(r'^[a-z]{2,3}(-[a-zA-Z]{2,3})?$', na=False))) * 100


def validate_currency(df, field):
    # Chequea moneda ISO-4217 (EUR, USD, etc.)
    return float(np.mean(df[field].str.match(r'^[A-Z]{3}$', na=False))) * 100


def validate_quality_metrics(df):
    # Calcula métricas principales y devuelve dict resumen
    return {
        "%_nulos_por_campo": null_percentage(df),
        "unicidad_book_id": uniqueness_check(df, "book_id"),
        "%_fechas_iso": validate_date_iso(df, "fecha_publicacion"),
        "%_idiomas_bcp47": validate_language(df, "idioma"),
        "%_monedas_iso4217": validate_currency(df, "moneda") if "moneda" in df else None,
    }
