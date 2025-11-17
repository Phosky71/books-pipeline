import re

import pandas as pd


def is_valid_isbn13(isbn):
    # Validación algoritmo estándar ISBN-13
    if not isbn or len(str(isbn)) != 13 or not str(isbn).isdigit():
        return False
    digits = [int(d) for d in str(isbn)]
    checksum = sum((d if i % 2 == 0 else d * 3) for i, d in enumerate(digits[:12]))
    check_digit = (10 - (checksum % 10)) % 10
    return check_digit == digits[12]


def is_valid_isbn10(isbn):
    # Validación estándar ISBN-10 con posible X check digit
    s = str(isbn).replace("-", "")
    if len(s) != 10: return False
    total = sum((i + 1) * (10 if x.upper() == 'X' else int(x)) for i, x in enumerate(s))
    return total % 11 == 0


def isbn10_to_isbn13(isbn10):
    # Conversión ISBN-10 a ISBN-13
    if not is_valid_isbn10(isbn10):
        return None
    core = '978' + str(isbn10)[:-1]
    checksum = sum((int(n) if i % 2 == 0 else int(n) * 3) for i, n in enumerate(core))
    last = (10 - (checksum % 10)) % 10
    return core + str(last)


def extract_isbn(text):
    # Extrae ISBNs de cadenas (útil para URLs)
    matches = re.findall(r"\b(\d{10}|\d{13})\b", text or "")
    isbn10 = next((m for m in matches if len(m) == 10), None)
    isbn13 = next((m for m in matches if len(m) == 13), None)
    return isbn10, isbn13


def normalize_isbn10(isbn):
    if pd.isna(isbn): return None
    x = str(isbn).strip().replace("-", "")
    return x if len(x) == 10 and x.isdigit() else None
