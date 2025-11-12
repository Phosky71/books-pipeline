# Goodreads Scraping

Este script realiza scraping de libros de Goodreads realizando una búsqueda pública (ejemplo: 'data science') y guarda los resultados en JSON para el pipeline.

## Configuración
- URL usada: https://www.goodreads.com/search?q=data+science
- Selectores principales (BeautifulSoup):
    - Título: `a.bookTitle span`
    - Autor: `a.authorName span`
    - Rating: `span.minirating`
    - Book URL: `a.bookTitle`
    - ISBN: No siempre visible, se trata de extraerlo de los enlaces o subpáginas
- User-Agent empleado: Mozilla/5.0 (compatible; AntonioBot/1.0)
- Fecha de scraping: (rellenar al ejecutar)
- Pausa entre peticiones: 0.7s
- N° de registros objetivos: >=12

## Ejecución
- Requiere requests, lxml, beautifulsoup4
- El resultado (landing/goodreads_books.json) debe incluir los campos:
  title, author, rating, ratings_count, book_url, isbn10, isbn13

## Notas
- Documenta la fecha y URL en el JSON y README.
- Marca los registros donde faltan datos clave.