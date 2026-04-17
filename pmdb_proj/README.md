
# PMDb dbt

Transforms raw movie data from Bronze → Silver → Gold using dbt Core and Snowflake.

## Pipeline

```
Bronze (raw)                    Silver (clean)            Gold (analytics)
─────────────                   ──────────────────        ─────────────────
LETTERBOXD_BASIC_IMPORT   ──┐
LETTERBOXD_POSTERS_IMPORT ──┴──→  LETTERBOXD_CLEAN   ──┐
IMDB_BASIC_IMPORT         ──┐                          │
IMDB_RATING_IMPORT        ──┴──→  IMDB_CLEAN           ├──→  JOINED_MOVIE_RATINGS
                                                    
```

## Models

### Silver

| Model               | Source             | Description                           |
| ------------------- | ------------------ | ------------------------------------- |
| `letterboxed_clean` | LETTERBOXD_IMPORT + LETTERBOXD_POSTERS_IMPORT | Filters nulls, casts date and runtime |
| `imdb_clean`  | IMDB_BASIC_IMPORT + IMDB_RATING_IMPORT | Filters to movies only, casts year    |

### Gold

| Model | Source | Description |
| --- | --- | --- |
| `movies_joined` |    `letterboxed_clean`+ `imdb_clean` |  Joins both silver tables on title + year, adds composite rating and rating diff columns |

## Usage

```bash
# Build all models
dbt run

# Run data quality tests
dbt test

# Run only Silver layer
dbt run --select silver

# Run only Gold layer
dbt run --select gold
```
