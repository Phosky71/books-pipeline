# Enriquecimiento Google Books

Este script toma el JSON de Goodreads y busca cada libro en la Google Books API por ISBN (preferente) o título+autor. Exporta los resultados a CSV (landing/googlebooks_books.csv) con los campos requeridos.

## Configuración
- API: Google Books v1
- Búsqueda por ISBN si está disponible; fallback a título+autor.
- Parámetros recogidos: gb_id, title, subtitle, authors, publisher, pub_date, language, categories, isbn13, isbn10, price_amount, price_currency
- Separador CSV: coma. Codificación: UTF-8
- Documentar hipótesis de mapeo en README y examples.

## Ejecución
- Requiere requests, pandas, python-dotenv para gestionar API key.
- Para cada libro del JSON, registrar metadatos relevantes y errores.

## Notas
- Priorizar registros con ISBN válido para mapeos exactos.