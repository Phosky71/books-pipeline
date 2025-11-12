# books-pipeline

Mini-pipeline para extraer, enriquecer e integrar datos de libros de Goodreads y Google Books, aplicando controles de calidad y criterios académicos según rúbrica SBDxx.

## Estructura del repositorio

```
books-pipeline/
├─ README.md
├─ requirements.txt
├─ .env.example
├─ landing/
│  ├─ goodreads_books.json
│  └─ googlebooks_books.csv
├─ standard/
│  ├─ dim_book.parquet
│  └─ book_source_detail.parquet
├─ docs/
│  ├─ schema.md
│  └─ quality_metrics.json
└─ src/
   ├─ scrape_goodreads.py
   ├─ enrich_googlebooks.py
   ├─ integrate_pipeline.py
   ├─ utils_quality.py
   └─ utils_isbn.py
```

## Descripción y pasos

1. **Scraping (Goodreads → JSON)**: obtención de libros por búsqueda pública, guardando fields clave y metadatos.
2. **Enriquecimiento (Google Books → CSV)**: búsqueda por ISBN/título+autor, captura de campos ampliados y mapeo.
3. **Integración (JSON+CSV → Parquet)**: normalización, deduplicación, controles de calidad, documentación y proveniencia.

## Ejecución

Instala dependencias:
```bash
pip install -r requirements.txt
```
Configura Google Books API key en `.env`. Ejecuta los scripts desde `src/`, generando artefactos en las carpetas correspondientes.

## Dependencias
- Python 3.10+
- requests, beautifulsoup4, lxml, pandas, pyarrow, numpy, python-dotenv

## Metadatos y decisiones
- Scraping documentado en README (URL, selectores, UA, fecha, nº registros)
- Separador CSV: coma; encoding: UTF-8
- Prioridad de búsqueda: ISBN > título+autor
- Normalización: fechas ISO-8601, idioma BCP-47, moneda ISO-4217

## Entregables
- Artefactos en `standard/` y `docs/`, README exportable a PDF para entrega SBDxx y link repositorio.

---

> Para detalles específicos de cada etapa, consulta el enunciado y la documentación en `docs/schema.md` y `docs/quality_metrics.json`.