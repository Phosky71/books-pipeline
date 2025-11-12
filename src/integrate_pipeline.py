# Integración pipeline JSON/CSV a Parquet

Este script integra ambas fuentes (JSON Goodreads, CSV Google Books) siguiendo controles de calidad y normalización. Emite datasets limpios y deduplicados para el modelo canónico: standard/dim_book.parquet, standard/book_source_detail.parquet, docs/quality_metrics.json, docs/schema.md

## Pasos principales
- Leer ambos archivos solo desde landing/.
- Registrar metadatos fuente, fecha ingesta, esquema detectado, recuentos de filas y tamaños.
- Chequear calidad: tipos, formatos ISO/BCP-47/ISO-4217, % nulos, rangos y claves requeridas.
- Modelo canónico: preferente isbn13; si falta, genera id estable con hash de title+author+publisher.
- Normalizar fechas, idioma, moneda. Aplicar trims y snake_case.
- Deduplicar según isbn13 o clave provisional; aplicar reglas de supervivencia (más completo, más reciente, fuente prioritaria).
- Merge de autores/categorías sin duplicados.
- Provenance por campo y registro. Logs claros.
- Fallar suave: registros con errores van a book_source_detail y métricas.
- Emitir todos artefactos en las rutas estándar.

## Dependencias
pandas, pyarrow, numpy, hashlib.

## Notas
No modificar landing/. Logs por archivo y regla. Documentar aserciones y métricas de calidad.
