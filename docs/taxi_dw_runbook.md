# Taxi DW runbook

## What this creates
- `main.taxi_dw.dim_city`
- `main.taxi_dw.dim_zipcode`
- `main.taxi_dw.dim_date`
- `main.taxi_dw.fact_taxi_rides`

## Source
- `samples.nyctaxi.trips` (verified columns: `tpep_pickup_datetime`, `tpep_dropoff_datetime`, `fare_amount`, `pickup_zip`, `dropoff_zip`)

## Current data profile
- Ride rows: 21,932
- Distinct zipcodes across pickup/dropoff: 206

## How to run
1. Open a Databricks SQL query editor attached to your SQL Warehouse.
2. Run `/Users/dvervarcke/Documents/New project/sql/001_build_taxi_dw.sql`.
3. Run `/Users/dvervarcke/Documents/New project/sql/002_enrich_city_from_external_zip.sql` once (optional bootstrap of city mappings).
4. Validate row counts:

```sql
SELECT 'dim_city' AS table_name, COUNT(*) AS rows FROM main.taxi_dw.dim_city
UNION ALL
SELECT 'dim_zipcode', COUNT(*) FROM main.taxi_dw.dim_zipcode
UNION ALL
SELECT 'dim_date', COUNT(*) FROM main.taxi_dw.dim_date
UNION ALL
SELECT 'fact_taxi_rides', COUNT(*) FROM main.taxi_dw.fact_taxi_rides;
```

## City handling
`trips` does not include city columns, so enrichment is done using external ZIP reference data (`zippopotam.us`) captured in `/Users/dvervarcke/Documents/New project/data/zip_city_mapping.csv`.

`main.taxi_dw.dim_city` includes:
- `city_key`
- `city_name`
- `state_code`
- `dw_loaded_at`

## Scheduled daily pipeline
- Databricks job: `taxi-dw-daily-incremental-load-and-enrichment`
- Job ID: `52643488824313`
- Pipeline file paths:
  - `/Users/rickoe@hotmail.com/taxi_dw/pipelines/run_incremental_dw_load.py`
  - `/Users/rickoe@hotmail.com/taxi_dw/pipelines/update_missing_cities.py`
- Schedule: daily at 07:00 `America/Los_Angeles` (PT)
- Logic:
  - Task 1 (`run-incremental-dw-load`): incrementally loads fact/date/zipcode using watermark + `MERGE`
  - Task 2 (`update-missing-cities`): runs after task 1 and enriches missing city mappings
  - Calls external ZIP API (`zippopotam.us`) only for unresolved ZIPs
  - Upserts `main.taxi_dw.zip_city_reference`, `main.taxi_dw.dim_city`, and `main.taxi_dw.dim_zipcode`

## Incremental fact/dim load
- `001_build_taxi_dw.sql` no longer rebuilds tables from scratch.
- It maintains `main.taxi_dw.etl_watermark` and processes only source deltas (with a 2-day lookback for late arrivals).
- It uses `MERGE` into:
  - `main.taxi_dw.fact_taxi_rides`
  - `main.taxi_dw.dim_date`
  - `main.taxi_dw.dim_zipcode`

Manual run:
```bash
databricks jobs run-now 52643488824313 --profile rickoe@hotmail.com
```
