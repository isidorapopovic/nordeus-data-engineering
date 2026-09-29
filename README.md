# Nordeus Data Engineering Pipeline

This project processes game event and map data from JSON Lines files into validated, analytics-ready player statistics. It uses PySpark for the Bronze and Silver data layers, then dbt with DuckDB for the Gold analytics models.

## Pipeline

1. **Raw input**: `data/raw/events.jsonl` and `data/raw/maps.jsonl`.
2. **Bronze** (`bronze.py`): reads the JSONL files with explicit schemas, retains malformed records for downstream checks, derives event timestamps and dates, and writes Parquet. Event data is appended incrementally by event ID; map data is refreshed.
3. **Silver** (`silver.py`): flattens event fields, normalizes values, validates required fields, and separates invalid rows into quarantine. It also validates maps and creates an enriched event dataset with map names. Event data is processed incrementally by event ID.
4. **Gold** (`nordeus_dbt/`): dbt models sessions and matches, calculates per-user and per-map metrics, and builds the `mart_player_stats` player-level table in DuckDB.

## Requirements

- Docker Desktop with Docker Compose
- Python with `dbt-duckdb` installed for the dbt step

Install the dbt adapter if needed:

```powershell
python -m pip install dbt-duckdb
```

## Run the pipeline

Run these commands from the repository root, in order:

```powershell
docker compose up -d spark
docker compose exec spark spark-submit /app/bronze.py
docker compose exec spark spark-submit /app/silver.py
Set-Location nordeus_dbt
dbt build --profiles-dir .
Set-Location ..
```

The dbt project reads the enriched Silver Parquet files, so complete both Spark steps before building the dbt models. `dbt build` runs the models and their configured tests.

## Data layout

```text
data/
  raw/                 Input JSONL files
  bronze/              Schema-applied Parquet data
  silver/              Validated events, maps, and enriched events
  quarantine/          Invalid event and map records
  gold/                Reserved for Gold outputs
nordeus_dbt/
  models/
    staging/           DuckDB views over Silver Parquet
    intermediate/      Session, match, and player metric models
    marts/             mart_player_stats
  tests/               Data quality SQL tests
  nordeus.duckdb       DuckDB development database
```

The player mart includes registration details, favorite map and its win ratio, total playtime, overall win ratio, and average matches per session. dbt tests check key uniqueness and non-null fields, non-negative playtime, and win ratios within the range 0 to 1.

## Main files

- `events.jsonl`, `maps.jsonl`: source files at the repository root; the pipeline reads their copies under `data/raw/`.
- `docker-compose.yml`: provides the Spark container with the repository mounted at `/app`.
- `bronze.py`, `silver.py`: Spark ingestion and transformation jobs.
- `nordeus_dbt/`: dbt project, DuckDB profile, models, and tests.
