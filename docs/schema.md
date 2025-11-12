# Esquema del modelo canónico y artefactos

## dim_book.parquet
Tabla canónica de libros (1 fila por libro)
- book_id (isbn13 o id_canónico),
- titulo
- titulo_normalizado
- autor_principal
- autores[]
- editorial
- anio_publicacion
- fecha_publicacion (ISO)
- idioma (BCP-47)
- isbn10
- isbn13
- paginas
- formato
- categoria[]
- precio
- moneda (ISO-4217)
- fuente_ganadora
- ts_ultima_actualizacion

## book_source_detail.parquet
Detalle por fuente y registro original
- source_id
- source_name
- source_file
- row_number
- book_id_candidato
- campos originales mapeados
- flags de validación
- timestamp de ingesta

## quality_metrics.json
Ejemplos:
- % filas válidas
- % fechas válidas
- % idiomas válidos
- % monedas válidas
- duplicados_encontrados
- nulos_por_campo
- filas_por_fuente

## Reglas de deduplicación/supervivencia
- Clave principal: isbn13 (o título/autor/editorial/año)
- Sobrevivencia: registro más completo/reciente/fuente prioritaria
- Unión de autores/categorías sin duplicados; merge de no nulos
- Provenance por campo

## Prioridades de fuentes
catalogo_interno > proveedor_A > scraping externos

## Formatos
- Fechas ISO-8601
- Idioma BCP-47
- Moneda ISO-4217
- Columnas snake_case
- Precios decimal, separador punto
- Trims y sets controlados

---

Documentación según rúbrica y ejemplo en README.