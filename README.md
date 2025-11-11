# Real Estate API & ETL Pipeline

## Overview

This project implements an incremental ETL pipeline that ingests real estate property data from a REST API into a Delta Lake with Bronze and Silver layers.

---

## API - Real Estate API

FastAPI-based REST API that provides real estate property data from Argentina, Paraguay, and Uruguay.

### Endpoints

- `GET /init` - Initialize database with 400 random properties
- `GET /new_houses` - Create 40 new properties with current timestamp
- `GET /houses/{from_date}/{to_date}` - Query properties by date range

### Date Formats

- Date only: `YYYY-MM-DD`
- DateTime: `YYYY-MM-DDTHH:MM:SS`

### Usage

```bash
cd api
pip install -r requirements.txt
python server.py
```

API runs on `http://localhost:8000`

---

## ETL Pipeline

Python-based ETL that loads data incrementally from the API into a Data Lake.

### Architecture

**Bronze Layer** (`datalake/bronze/realestateapi/`)
- Raw data from API in Delta format
- Partitioned by `published_date`
- Incremental load based on max published date

**Silver Layer** (`datalake/silver/realestateapi/`)
- Curated data in Delta table format
- UPSERT using `id` and `published_at` as predicates
- Ready for analytics

### Technologies

- **requests** - API extraction
- **pandas** - Data transformation
- **deltalake** - Delta Lake storage

### Usage

```bash
cd etl
pip install -r requirements.txt
python main.py
```

### Incremental Logic

1. **First run**: Loads all data from `1990-01-01` to today
2. **Subsequent runs**: Loads only new data from max date in Bronze
3. **Silver layer**: Only runs if new data exists in Bronze

---

## Workflow

1. Start API: `cd api && python server.py`
2. Initialize data: `curl http://localhost:8000/init`
3. Run ETL: `cd etl && python main.py`
4. Generate new data: `curl http://localhost:8000/new_houses`
5. Run ETL again for incremental load: `python main.py`
