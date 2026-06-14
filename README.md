# books-pipeline

A Python ETL pipeline that extracts, enriches, and consolidates book data from Goodreads and the Google Books API into a clean, canonical Parquet model. The pipeline covers the full data lifecycle: web scraping, API-based enrichment, deduplication, schema standardization, and quality control.

> **Status:** Core pipeline functional. All three stages (scrape → enrich → integrate) produce validated output artifacts.

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Features](#features)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Getting Started](#getting-started)
- [Pipeline Stages](#pipeline-stages)
- [Data Model](#data-model)
- [Quality Control](#quality-control)
- [Contributing](#contributing)
- [License](#license)

## Overview

`books-pipeline` automates the process of collecting and standardizing book metadata from two complementary sources:

- **Goodreads** — scraped via BeautifulSoup to collect titles, authors, ratings, genres, and ISBNs.
- **Google Books API** — used to enrich records with additional metadata (publisher, page count, language, description) by querying with ISBN or title/author.

The final output is a canonical dimensional model stored in Apache Parquet format, suitable for analytics, reporting, or downstream ML workflows.

## Architecture

```
Goodreads (HTML)
       │
       ▼
 scrape_goodreads.py  ──►  landing/goodreads_books.json
       │
       ▼
enrich_googlebooks.py ──►  landing/googlebooks_books.csv
       │
       ▼
integrate_pipeline.py ──►  standard/dim_book.parquet
                           standard/book_source_detail.parquet
```

## Features

- **Web scraping** of Goodreads book listings with configurable search queries
- **API enrichment** via Google Books, with ISBN-first lookup and title/author fallback
- **Deduplication** based on ISBN and title normalization
- **Schema standardization** into a canonical book dimension model
- **Quality metrics** generation with field coverage and completeness reporting
- **Environment-based configuration** via `.env` for API keys and settings
- **Parquet output** for efficient columnar storage and analytics compatibility

## Tech Stack

| Layer | Technology |
|---|---|
| Language | Python 3.10+ |
| Scraping | `requests`, `beautifulsoup4`, `lxml` |
| Data processing | `pandas`, `numpy` |
| Storage | `pyarrow` (Parquet), JSON, CSV |
| Configuration | `python-dotenv` |
| API | Google Books REST API |

## Project Structure

```
books-pipeline/
├── src/
│   ├── scrape_goodreads.py      # Stage 1: Scrape Goodreads
│   ├── enrich_googlebooks.py    # Stage 2: Enrich via Google Books API
│   ├── integrate_pipeline.py    # Stage 3: Integrate, deduplicate, and export
│   ├── utils_isbn.py            # ISBN validation and normalization utilities
│   └── utils_quality.py        # Quality metrics computation
├── landing/
│   ├── goodreads_books.json     # Raw scraped data
│   └── googlebooks_books.csv   # Enriched API data
├── standard/
│   ├── dim_book.parquet         # Canonical book dimension
│   └── book_source_detail.parquet  # Source provenance detail
├── docs/
│   ├── schema.md                # Field definitions and data dictionary
│   └── quality_metrics.json    # Automated quality report
├── .env.example                 # Environment variable template
├── requirements.txt
└── README.md
```

## Getting Started

### Prerequisites

- Python 3.10+
- A Google Books API key ([get one here](https://developers.google.com/books/docs/v1/using#APIKey))

### Installation

```bash
git clone https://github.com/Phosky71/books-pipeline.git
cd books-pipeline
pip install -r requirements.txt
```

### Configuration

Copy `.env.example` to `.env` and fill in your credentials:

```bash
cp .env.example .env
```

```env
GOOGLE_BOOKS_API_KEY=your_api_key_here
```

### Running the Pipeline

Execute each stage in order:

```bash
# Stage 1 - Scrape Goodreads
python src/scrape_goodreads.py

# Stage 2 - Enrich with Google Books API
python src/enrich_googlebooks.py

# Stage 3 - Integrate and export to Parquet
python src/integrate_pipeline.py
```

Output artifacts will be written to `landing/` (raw) and `standard/` (processed).

## Pipeline Stages

### Stage 1 — Scrape (`scrape_goodreads.py`)

Extracts book records from Goodreads public listings. Fields captured include title, author(s), average rating, number of ratings, genres, and ISBN. Output is persisted to `landing/goodreads_books.json`.

### Stage 2 — Enrich (`enrich_googlebooks.py`)

Looks up each scraped book in the Google Books API. Primary key is ISBN; falls back to title + author search if ISBN is missing. Captured fields include publisher, publication date, page count, language, and description. Output is `landing/googlebooks_books.csv`.

### Stage 3 — Integrate (`integrate_pipeline.py`)

Merges both sources, deduplicates by ISBN and normalized title, applies type casting and field standardization, then exports to two Parquet tables:

- `dim_book.parquet` — canonical book records
- `book_source_detail.parquet` — source provenance and field-level completeness

## Data Model

### `dim_book.parquet`

| Field | Type | Description |
|---|---|---|
| `isbn` | string | Primary identifier (ISBN-13 preferred) |
| `title` | string | Normalized book title |
| `authors` | list[string] | List of author names |
| `publisher` | string | Publisher name |
| `published_date` | date | First publication date |
| `page_count` | int | Number of pages |
| `language` | string | ISO 639-1 language code |
| `average_rating` | float | Goodreads average rating |
| `ratings_count` | int | Number of Goodreads ratings |
| `description` | string | Book synopsis |
| `genres` | list[string] | Genre tags from Goodreads |

## Quality Control

The `utils_quality.py` module computes a quality report after integration, written to `docs/quality_metrics.json`. Metrics include:

- Field completeness rate (% of non-null values per column)
- Deduplication rate
- Source coverage (records enriched by Google Books vs. Goodreads-only)
- Schema conformance validation

## Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/your-feature`
3. Commit your changes: `git commit -m 'feat: add your feature'`
4. Push to the branch: `git push origin feature/your-feature`
5. Open a Pull Request

Please follow [Conventional Commits](https://www.conventionalcommits.org/) for commit messages.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.
